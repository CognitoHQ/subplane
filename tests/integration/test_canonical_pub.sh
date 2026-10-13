#!/usr/bin/env bash
# The sample publisher (examples/canonical_pub.cpp) -> flashtickd -> ring -> views_app.
# usage: test_canonical_pub.sh <canonical_pub> <flashtickd> <views_app> <schema.xml>
set -u
PUB=$1; AGENT=$2; APP=$3; SCHEMA=$4
TMP=$(mktemp -d); trap 'rm -rf "$TMP"; kill $(jobs -p) 2>/dev/null; rm -f /dev/shm/flashtick.pubtest.*.ring' EXIT
fail() { echo "FAIL: $*"; tail -30 "$TMP"/*.log 2>/dev/null; exit 1; }
http_get() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'GET %s HTTP/1.0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
PORT=$(( 20000 + (RANDOM % 30000) )); API=$(( 20000 + (RANDOM % 30000) )); SOCK="$TMP/ctl.sock"
cat > "$TMP/config.yaml" <<YAML
agent: { name: pubtest, version: test, max_symbols: 65536, thread_groups: [ { name: g0, cpu: -1, partitions: [can0] } ] }
partitions:
  - name: can0
    transport: { type: udp, group: "", port: $PORT, interface: "", rcvbuf: 4194304 }
    framing: {packet_header: [{name: seq, type: u64le}, {name: count, type: u16le}], message_prefix: [{name: len, type: u16le}], sequence: packet_header.seq, length: message_prefix.len}
    decoder:
      type: sbe_routing
      schema: $SCHEMA
      fields: { symbol: InstrumentId, sequence: Sequence, exchange_ts: ExchangeTs, venue: VenueId }
      symbol_id_mode: table
      messages: [ { template: 1, msg_type: TRADE }, { template: 2, msg_type: QUOTE }, { template: 100, admin: HEARTBEAT } ]
    heartbeat_timeout_ms: 2000
sessions:
  - { name: all, ring: { capacity_mb: 8, max_payload: 4096 }, partitions: [can0], symbols: ["*"], msg_types: [], deliver: { bus_admin: true, agent_control: true } }
fanout: { ring_dir: /dev/shm }
control: { socket_path: $SOCK, fallback_socket_path: $SOCK, linger_ms: 1000 }
lvc: { max_payload: 512 }
api: { enabled: true, host: 127.0.0.1, port: $API }
metrics: { enabled: false, report_interval_seconds: 60, output_format: json, console_output: false }
logging: { level: info, console_output: true, output_format: text }
YAML
"$AGENT" --config "$TMP/config.yaml" > "$TMP/agent.log" 2>&1 & AGENT_PID=$!
for _ in $(seq 1 100); do http_get "$API" /status >/dev/null 2>&1 && break; sleep 0.1; done
kill -0 $AGENT_PID 2>/dev/null || fail "agent exited early"
"$PUB" --port "$PORT" --count 1000 --rate 10000 > "$TMP/pub.log" 2>&1 || fail "publisher failed"
grep -q "sent 1000 messages in 500 packets" "$TMP/pub.log" || fail "publisher report: $(cat "$TMP/pub.log")"
RING=/dev/shm/flashtick.pubtest.all.ring
for _ in $(seq 1 50); do [ -f "$RING" ] && break; sleep 0.1; done
"$APP" "$RING" --from-start --quiet --seconds 2 > "$TMP/app.log" 2>&1 || fail "views_app exit $?"
SUMMARY=$(tail -1 "$TMP/app.log"); echo "$SUMMARY"
[ "$SUMMARY" = "trades=500 quotes=500 books=0 snapshots=0 other=0 heartbeats=0 bad=0" ] || fail "unexpected summary"
kill $AGENT_PID; wait $AGENT_PID 2>/dev/null
echo "PASS"
