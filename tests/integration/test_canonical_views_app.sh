#!/usr/bin/env bash
# Canonical schema end to end: subplane send (canonical profile) -> subplaned -> ring -> views_app
# (generated struct views). Needs only bash, grep and sed.
# usage: test_canonical_views_app.sh <subplane send> <subplaned> <views_app> <schema.xml>
set -u
SP=$1; AGENT=$2; APP=$3; SCHEMA=$4
TMP=$(mktemp -d); trap 'rm -rf "$TMP"; kill $(jobs -p) 2>/dev/null; rm -f /dev/shm/subplane.cantest.*.ring' EXIT
fail() { echo "FAIL: $*"; tail -30 "$TMP"/*.log 2>/dev/null; exit 1; }
http_get() ( # port path
    trap '' PIPE
    exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1
    printf 'GET %s HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n' "$2" >&3 || return 1
    tr -d '\r' <&3 | sed '1,/^$/d'
    exec 3<&-
)
PORT=$(( 20000 + (RANDOM % 30000) ))
API=$(( 20000 + (RANDOM % 30000) ))
SOCK="$TMP/ctl.sock"
FRAMING='{packet_header: [{name: seq, type: u64le}, {name: count, type: u16le}], message_prefix: [{name: len, type: u16le}], sequence: packet_header.seq, length: message_prefix.len}'
cat > "$TMP/config.yaml" <<YAML
agent: { name: cantest, version: test, max_symbols: 65536, thread_groups: [ { name: g0, cpu: -1, partitions: [can0] } ] }
partitions:
  - name: can0
    transport: { type: udp, group: "", port: $PORT, interface: "", rcvbuf: 4194304 }
    framing: $FRAMING
    decoder:
      type: sbe_routing
      schema: $SCHEMA
      fields: { symbol: InstrumentId, sequence: Sequence, exchange_ts: ExchangeTs, venue: VenueId }
      symbol_id_mode: table
      messages:
        - { template: 1, msg_type: TRADE }
        - { template: 2, msg_type: QUOTE }
        - { template: 3, msg_type: BOOK_UPDATE }
        - { template: 4, msg_type: BOOK_SNAPSHOT }
        - { template: 5, msg_type: OHLCV }
        - { template: 6, msg_type: STATUS }
        - { template: 10, msg_type: DEFINITION }
        - { template: 100, admin: HEARTBEAT }
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
"$AGENT" --config "$TMP/config.yaml" > "$TMP/agent.log" 2>&1 &
AGENT_PID=$!
for _ in $(seq 1 100); do http_get "$API" /status >/dev/null 2>&1 && break; sleep 0.1; done
kill -0 $AGENT_PID 2>/dev/null || fail "agent exited early"
"$SP" send --schema "$SCHEMA" --framing "$FRAMING" --port "$PORT" --count 1000 --per-packet 2 --rate 20000 \
          --mix trade=60,quote=30,book=10 --heartbeat-ms 20 --symbols 5 > "$TMP/sender.log" 2>&1 || fail "sender failed"
grep -q "packets=1000" "$TMP/sender.log" || fail "sender did not report 1000 packets: $(cat "$TMP/sender.log")"
RING=/dev/shm/subplane.cantest.all.ring
for _ in $(seq 1 50); do [ -f "$RING" ] && break; sleep 0.1; done
[ -f "$RING" ] || fail "ring missing"
"$APP" "$RING" --from-start --quiet --seconds 3 > "$TMP/app.log" 2>&1; RC=$?
SUMMARY=$(tail -1 "$TMP/app.log"); echo "$SUMMARY"
num() { sed -n "s/.*$1=\([0-9]*\).*/\1/p" <<<"$SUMMARY"; }
T=$(num trades); Q=$(num quotes); B=$(num books); HB=$(num heartbeats); BAD=$(num bad)
[ "$RC" -eq 0 ] || fail "views_app exit $RC"
[ $((T + Q + B)) -eq 2000 ] || fail "expected 2000 data records through the views, got trades=$T quotes=$Q books=$B"
[ "$T" -gt 800 ] && [ "$Q" -gt 300 ] && [ "$B" -gt 50 ] || fail "mix looks wrong: trades=$T quotes=$Q books=$B"
[ "${HB:-0}" -ge 1 ] || fail "no heartbeats"
[ "${BAD:-1}" -eq 0 ] || fail "bad=$BAD"
# The cold API answers on its own thread; retry the read like the other integration scripts do.
UNK=""
for _ in $(seq 1 50); do
    PART=$(http_get "$API" /partitions 2>/dev/null) && UNK=$(sed -n 's/.*"unknown_templates":[[:space:]]*\([0-9]*\).*/\1/p' <<<"$PART" | head -1)
    [ -n "$UNK" ] && break
    sleep 0.1
done
[ -n "$UNK" ] || fail "no /partitions answer"
[ "$UNK" -eq 0 ] || fail "unknown_templates=$UNK"
kill $AGENT_PID; wait $AGENT_PID 2>/dev/null
echo "PASS"
