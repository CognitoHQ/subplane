#!/usr/bin/env bash
# subplane aeron-pub -> Aeron media driver -> subplaned (`aeron` transport) -> ring -> views_app.
# usage: test_aeron_ingest.sh <ipc|udp> <subplane aeron-pub> <subplaned> <views_app> <schema.xml>
#   ipc: the agent embeds the media driver; the publisher attaches to its directory.
#   udp: publisher and agent each embed a driver (two directories) and talk over UDP loopback.
set -u
MODE=$1; SP=$2; AGENT=$3; APP=$4; SCHEMA=$5
# INTENT: the agent name (and so the ring path) is per mode; the ipc and udp
# runs share /dev/shm, and under a parallel ctest one would read, then delete,
# the other's ring.
TMP=$(mktemp -d); trap 'rm -rf "$TMP"; kill $(jobs -p) 2>/dev/null; rm -f /dev/shm/subplane.aerontest_$MODE.*.ring' EXIT
fail() { echo "FAIL: $*"; tail -30 "$TMP"/*.log 2>/dev/null; exit 1; }
http_get() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'GET %s HTTP/1.0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
PORT=$(( 20000 + (RANDOM % 30000) )); API=$(( 20000 + (RANDOM % 30000) )); SOCK="$TMP/ctl.sock"
AGENT_DIR="$TMP/aeron-agent"; PUB_DIR="$TMP/aeron-pub"
if [ "$MODE" = ipc ]; then
  CHANNEL="aeron:ipc"; PUB_ARGS=(--aeron-dir "$AGENT_DIR")
else
  CHANNEL="aeron:udp?endpoint=127.0.0.1:$PORT"; PUB_ARGS=(--aeron-dir "$PUB_DIR" --embedded-driver)
fi
cat > "$TMP/config.yaml" <<YAML
agent: { name: aerontest_$MODE, version: test, max_symbols: 65536, thread_groups: [ { name: g0, cpu: -1, partitions: [aer0] } ] }
partitions:
  - name: aer0
    transport: { type: aeron, channel: "$CHANNEL", stream_id: 1001, aeron_dir: "$AGENT_DIR", driver: { embedded: true, threading_mode: shared } }
    framing: { length: packet, sequence: message.Sequence }
    decoder:
      type: sbe_routing
      schema: $SCHEMA
      fields: { symbol: InstrumentId, sequence: Sequence, exchange_ts: ExchangeTs, venue: VenueId }
      symbol_id_mode: table
      messages: [ { template: 1, msg_type: TRADE }, { template: 2, msg_type: QUOTE }, { template: 100, admin: HEARTBEAT } ]
    heartbeat_timeout_ms: 2000
sessions:
  - { name: all, ring: { capacity_mb: 8, max_payload: 4096 }, partitions: [aer0], symbols: ["*"], msg_types: [], deliver: { bus_admin: true, agent_control: true } }
fanout: { ring_dir: /dev/shm }
control: { socket_path: $SOCK, fallback_socket_path: $SOCK, linger_ms: 1000 }
lvc: { max_payload: 512 }
api: { enabled: true, host: 127.0.0.1, port: $API }
metrics: { enabled: false, report_interval_seconds: 60, output_format: json, console_output: false }
logging: { level: info, console_output: true, output_format: text }
YAML
"$AGENT" --config "$TMP/config.yaml" > "$TMP/agent.log" 2>&1 & AGENT_PID=$!
for _ in $(seq 1 150); do http_get "$API" /status >/dev/null 2>&1 && break; sleep 0.1; done
kill -0 $AGENT_PID 2>/dev/null || fail "agent exited early"
"$SP" aeron-pub --channel "$CHANNEL" --stream-id 1001 "${PUB_ARGS[@]}" --count 1000 --rate 20000 --heartbeat-every 100 > "$TMP/pub.log" 2>&1 || fail "publisher failed"
grep -q "sent 1000 messages and 10 heartbeats" "$TMP/pub.log" || fail "publisher report: $(cat "$TMP/pub.log")"
RING=/dev/shm/subplane.aerontest_$MODE.all.ring
for _ in $(seq 1 50); do [ -f "$RING" ] && break; sleep 0.1; done
"$APP" "$RING" --from-start --quiet --seconds 2 > "$TMP/app.log" 2>&1 || fail "views_app exit $?"
SUMMARY=$(tail -1 "$TMP/app.log"); echo "$SUMMARY"
[ "$SUMMARY" = "trades=500 quotes=500 books=0 snapshots=0 other=0 heartbeats=10 bad=0" ] || fail "unexpected summary"
STATUS=$(http_get "$API" /partitions); echo "$STATUS" | grep -q '"packets":1010,"messages":1010,"unknown_templates":0,"gaps":0' || fail "unexpected partition status: $STATUS"
grep -q "aeron $CHANNEL stream 1001 (embedded driver)" "$TMP/agent.log" || fail "agent log lacks the aeron transport description"
# The upstream endpoint key, and the Aeron metrics (docs/context/observability.md).
H=$(hostname | tr 'A-Z' 'a-z')
if [ "$MODE" = ipc ]; then KEY="aeron-ipc://$H$AGENT_DIR#1001"; else KEY="aeron-udp://$H:$PORT#1001"; fi
http_get "$API" /topology | grep -qF '"key":"'"$KEY"'","role":"data"' || fail "topology key: want $KEY in $(http_get "$API" /topology)"
http_get "$API" /metrics | grep -q '^subplane_transport_fragments_total{.*partition="aer0"} 1010$' || fail "aeron fragments metric: $(http_get "$API" /metrics | grep fragments)"
# Transport state events. IPC: the image goes within about a second of the
# publisher exiting (TRANSPORT_DOWN, source lost). UDP: the image outlives the
# publisher, so only the restart is seen. Either way a second publisher run is
# a new Aeron session: TRANSPORT_UP with reason publisher_restart.
if [ "$MODE" = ipc ]; then
  for _ in $(seq 1 50); do http_get "$API" /partitions | grep -q '"transport":"down"' && break; sleep 0.1; done
  http_get "$API" /partitions | grep -q '"transport":"down"' || fail "transport not down after the publisher exited: $(http_get "$API" /partitions)"
fi
"$SP" aeron-pub --channel "$CHANNEL" --stream-id 1001 "${PUB_ARGS[@]}" --count 10 --rate 20000 > "$TMP/pub2.log" 2>&1 || fail "second publisher failed"
# The short second run may already be gone again, and a plain UP may precede
# the restart UP (see architecture.md, 0xA8), so check the events, not counts.
for _ in $(seq 1 50); do
  "$SP" tail "$RING" --from-start > "$TMP/tail.log" 2>&1   # drains the ring and exits
  grep -aq "reason=publisher_restart" "$TMP/tail.log" && break; sleep 0.1
done
grep -a TRANSPORT "$TMP/tail.log"
grep -aq "TRANSPORT_UP .* reason=none last_seq=0" "$TMP/tail.log" || fail "no first TRANSPORT_UP"
grep -aq "TRANSPORT_UP .* reason=publisher_restart" "$TMP/tail.log" || fail "no TRANSPORT_UP for the publisher restart"
if [ "$MODE" = ipc ]; then
  grep -aq "TRANSPORT_DOWN .* reason=source_lost last_seq=1010" "$TMP/tail.log" || fail "no TRANSPORT_DOWN when the publisher went away"
fi
kill $AGENT_PID; wait $AGENT_PID 2>/dev/null
echo "PASS"
