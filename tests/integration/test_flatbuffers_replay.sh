#!/usr/bin/env bash
# FlatBuffers over UDP end to end: subplane replay --pcap (real flatc buffers, 1..4 per datagram)
# -> subplaned (udp + message_prefix/keep_prefix + flatbuffers decoder) -> ring.
# Needs only bash, grep and sed (no curl/python: the CI container is minimal).
# usage: test_flatbuffers_replay.sh <subplane> <subplaned> <fixture dir> [capture file, default fb_sample.pcap]
# The same fixture is checked in as classic pcap and as pcapng (fb_sample.pcapng), and both must replay alike.
set -u
SP=$1; AGENT=$2; FIX=$3; CAP=${4:-fb_sample.pcap}
NAME=fbtest$( [ "$CAP" = fb_sample.pcap ] || echo ng )
TMP=$(mktemp -d); trap 'rm -rf "$TMP"; kill $(jobs -p) 2>/dev/null; rm -f /dev/shm/subplane.$NAME.*.ring /dev/shm/subplane.$NAME.*.ctx' EXIT
fail() { echo "FAIL: $*"; tail -30 "$TMP"/*.log 2>/dev/null; exit 1; }

# Runs in a subshell so a failed connect (nothing listening yet) only fails the
# call: a failed `exec` redirection would exit a non-interactive bash. SIGPIPE is
# ignored so a peer that closes early yields a write error, not a dead script.
http_get() ( # port path
    trap '' PIPE
    exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1
    printf 'GET %s HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n' "$2" >&3 || return 1
    tr -d '\r' <&3 | sed '1,/^$/d'
    exec 3<&-
)
json_num() { sed -n "s/.*\"$1\":[[:space:]]*\([0-9][0-9]*\).*/\1/p" <<<"$2" | head -1; }

PORT=$(( 20000 + (RANDOM % 30000) ))
API=$(( 20000 + (RANDOM % 30000) ))
SOCK="$TMP/ctl.sock"
cat > "$TMP/config.yaml" <<YAML
agent: { name: $NAME, version: test, max_symbols: 65536, thread_groups: [ { name: g0, cpu: -1, partitions: [fb0] } ] }
partitions:
  - name: fb0
    transport: { type: udp, group: "", port: $PORT, interface: "", rcvbuf: 4194304 }
    framing: { message_prefix: [ { name: size, type: u32le } ], length: message_prefix.size, keep_prefix: true }
    decoder:
      type: flatbuffers
      dispatch: { type_field: { path: [2], type: u8 } }
      messages:
        - { type: 1, msg_type: TRADE, symbol: { path: [3, 0, 0], type: u32 }, venue: { path: [3, 0, 1], type: u16 }, exchange_ts: { path: [1], type: u64 }, sequence: { path: [0], type: u64 } }
        - { type: 2, msg_type: QUOTE, symbol: { path: [3, 0, 0], type: u32 }, venue: { path: [3, 0, 1], type: u16 }, exchange_ts: { path: [1], type: u64 }, sequence: { path: [0], type: u64 } }
        - { type: 3, admin: HEARTBEAT, sequence: { path: [0], type: u64 } }
    heartbeat_timeout_ms: 2000
    tap: { capacity_mb: 8 }
sessions:
  - { name: three, ring: { capacity_mb: 8, max_payload: 4096 }, partitions: [fb0], symbols: ["1000", "1006", "1031"], msg_types: [TRADE], deliver: { bus_admin: true, agent_control: true } }
fanout: { ring_dir: /dev/shm }
control: { socket_path: $SOCK, fallback_socket_path: $SOCK, linger_ms: 1000 }
lvc: { max_payload: 512 }
api: { enabled: true, host: 127.0.0.1, port: $API }
metrics: { enabled: false, report_interval_seconds: 60, output_format: json, console_output: false }
logging: { level: info, console_output: true, output_format: text }
YAML

"$AGENT" --config "$TMP/config.yaml" > "$TMP/agent.log" 2>&1 &
AGENT_PID=$!
# Wait for the socket to be bound before replaying.
for _ in $(seq 1 100); do http_get "$API" /status >/dev/null 2>&1 && break; sleep 0.1; done
kill -0 $AGENT_PID 2>/dev/null || fail "agent exited early"
"$SP" replay --pcap "$FIX/$CAP" --port "$PORT" --host 127.0.0.1 --rate 20000 > "$TMP/replay.log" 2>&1 || fail "subplane replay failed"

PART=""; MSGS=""
for _ in $(seq 1 100); do
    PART=$(http_get "$API" /partitions 2>/dev/null) && MSGS=$(json_num messages "$PART")
    [ -n "$MSGS" ] && [ "$MSGS" -ge 300 ] && break
    sleep 0.1
done
[ -n "$PART" ] || fail "no /partitions"
UNK=$(json_num unknown_templates "$PART"); DERR=$(json_num decode_errors "$PART"); PKTS=$(json_num packets "$PART")
[ "${MSGS:-0}" -eq 300 ] || fail "expected 300 messages, got '$MSGS'"
[ "${PKTS:-0}" -eq 120 ] || fail "expected 120 packets, got '$PKTS'"
[ "${UNK:-0}" -eq 0 ] || fail "unknown_templates=$UNK"
[ "${DERR:-0}" -eq 0 ] || fail "decode_errors=$DERR"
echo "partition ok: $MSGS messages in $PKTS packets"

RING=/dev/shm/subplane.$NAME.three.ring
[ -f "$RING" ] || fail "ring missing"
"$SP" tail "$RING" --from-start --count 100000 > "$TMP/tail.log" 2>&1 || fail "subplane tail failed"
DATA=$(grep -c " kind=0 " "$TMP/tail.log")
WANT=$(grep -c "^1 [0-9]* [0-9]* \(1000\|1006\|1031\) " "$FIX/fb_sample.expected")
HB=$(grep -c "BUS_HEARTBEAT" "$TMP/tail.log")
BAD=$(grep " kind=0 " "$TMP/tail.log" | grep -vc "sym=\(1000\|1006\|1031\) ")
QUOTES=$(grep -c " QUOTE " "$TMP/tail.log")
[ "$DATA" -eq "$WANT" ] || fail "expected $WANT trade records on the ring, got $DATA"
[ "$HB" -eq 30 ] || fail "expected 30 heartbeats on the ring, got $HB"
[ "$BAD" -eq 0 ] || fail "$BAD records for instruments outside the filter"
[ "$QUOTES" -eq 0 ] || fail "$QUOTES QUOTE records despite the msg_types filter"
echo "ring ok: trades=$DATA heartbeats=$HB"

# The tap: every datagram, as it arrived, for a recorder.
TAP=/dev/shm/subplane.$NAME.tap.fb0.ring
[ -f "$TAP" ] && [ -f "/dev/shm/subplane.$NAME.tap.fb0.ctx" ] || fail "tap ring or context missing"
TAPJ=$(sed -n 's/.*"tap":{\([^}]*\)}.*/\1/p' <<<"$PART")
[ "$(json_num records "{$TAPJ}")" = 120 ] || fail "tap records: $TAPJ"
[ "$(json_num dropped "{$TAPJ}")" = 0 ] || fail "tap dropped: $TAPJ"
"$SP" tail "$TAP" --from-start --count 100000 > "$TMP/tap.log" 2>&1 || fail "subplane tail on the tap failed"
RAW=$(grep -c " RAW " "$TMP/tap.log")
[ "$RAW" -eq 120 ] || fail "expected 120 RAW records on the tap, got $RAW"
http_get "$API" /topology | grep -q '"role":"record"' || fail "no record edge in /topology"
echo "tap ok: $RAW units"
kill $AGENT_PID; wait $AGENT_PID 2>/dev/null
echo "PASS"
