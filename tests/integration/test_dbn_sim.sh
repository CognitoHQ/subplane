#!/usr/bin/env bash
# DBN over TCP end to end: ft_dbn_sim (synthetic) -> flashtickd (tcp_stream + dbn) -> ring.
# Needs only bash, grep and sed (no curl/python: the CI container is minimal).
# usage: test_dbn_sim.sh <ft_dbn_sim> <flashtickd> <ft_tail>
set -u
SIM=$1; AGENT=$2; TAIL=$3
TMP=$(mktemp -d); trap 'rm -rf "$TMP"; kill $(jobs -p) 2>/dev/null; rm -f /dev/shm/flashtick.dbntest.*.ring' EXIT
fail() { echo "FAIL: $*"; tail -30 "$TMP"/*.log 2>/dev/null; exit 1; }

# Minimal HTTP GET over bash's /dev/tcp; prints the body.
http_get() { # port path
    exec 3<>"/dev/tcp/127.0.0.1/$1" || return 1
    printf 'GET %s HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n' "$2" >&3
    tr -d '\r' <&3 | sed '1,/^$/d'
    exec 3<&-
}
json_num() { # key json -> first integer value of "key": N
    sed -n "s/.*\"$1\":[[:space:]]*\([0-9][0-9]*\).*/\1/p" <<<"$2" | head -1
}

PORT=$(( 20000 + (RANDOM % 30000) ))
API=$(( 20000 + (RANDOM % 30000) ))
SOCK="$TMP/ctl.sock"
cat > "$TMP/config.yaml" <<EOF
agent: { name: dbntest, version: test, max_symbols: 65536, thread_groups: [ { name: g0, cpu: -1, partitions: [db0] } ] }
partitions:
  - name: db0
    transport: { type: tcp_stream, host: 127.0.0.1, port: $PORT, record_length: { offset: 0, size: 1, scale: 4 }, prelude: dbn, reconnect_ms: 100 }
    decoder: { type: dbn }
    heartbeat_timeout_ms: 2000
sessions:
  - { name: three, ring: { capacity_mb: 8, max_payload: 4096 }, partitions: [db0], symbols: ["1000", "1001", "1002"], msg_types: [TRADE, BOOK_UPDATE], deliver: { bus_admin: true, agent_control: true } }
fanout: { ring_dir: /dev/shm }
control: { socket_path: $SOCK, fallback_socket_path: $SOCK, linger_ms: 1000 }
lvc: { max_payload: 512 }
api: { enabled: true, host: 127.0.0.1, port: $API }
metrics: { enabled: false, report_interval_seconds: 60, output_format: json, console_output: false }
logging: { level: info, console_output: true, output_format: text }
EOF

# Simulator first (the agent reconnects until it is up anyway), one client, then exit.
"$SIM" --port "$PORT" --synthetic --instruments 10 --count 5000 --mix mbo=60,trade=30,mbp1=10 \
       --heartbeat-ms 50 --rate 20000 --clients 1 > "$TMP/sim.log" 2>&1 &
SIM_PID=$!
"$AGENT" --config "$TMP/config.yaml" > "$TMP/agent.log" 2>&1 &
AGENT_PID=$!

# Wait for the simulator to finish serving its one client (5000 records at 20k/s ~ 0.25 s).
for _ in $(seq 1 100); do kill -0 $SIM_PID 2>/dev/null || break; sleep 0.1; done
kill -0 $SIM_PID 2>/dev/null && fail "simulator did not finish"
kill -0 $AGENT_PID 2>/dev/null || fail "agent exited early"

# The API server starts after the transports, so on a slow runner the stream
# can be fully consumed before /partitions answers: retry until it does and
# until the counters have caught up.
PART=""; MSGS=""
for _ in $(seq 1 100); do
    PART=$(http_get "$API" /partitions 2>/dev/null) && MSGS=$(json_num messages "$PART")
    [ -n "$MSGS" ] && [ "$MSGS" -ge 5000 ] && break
    sleep 0.1
done
[ -n "$PART" ] || fail "no /partitions"
echo "$PART" > "$TMP/partitions.json"
UNK=$(json_num unknown_templates "$PART")
DERR=$(json_num decode_errors "$PART"); TERR=$(json_num transport_errors "$PART")
[ -n "$MSGS" ] && [ "$MSGS" -ge 5000 ] || fail "expected >= 5000 messages, got '$MSGS'"
[ "${UNK:-0}" -eq 0 ] || fail "unknown_templates=$UNK"
[ "${DERR:-0}" -eq 0 ] || fail "decode_errors=$DERR"
[ "${TERR:-0}" -eq 0 ] || fail "transport_errors=$TERR"
grep -q '"state": *"active"' <<<"$PART" || fail "partition not active"
echo "partition ok: $MSGS messages"

RING=/dev/shm/flashtick.dbntest.three.ring
[ -f "$RING" ] || fail "ring missing"
"$TAIL" "$RING" --from-start --count 100000 > "$TMP/tail.log" 2>&1 || fail "ft_tail failed"
DATA=$(grep -c " kind=0 " "$TMP/tail.log")
HB=$(grep -c "BUS_HEARTBEAT" "$TMP/tail.log")
BAD=$(grep " kind=0 " "$TMP/tail.log" | grep -vc "sym=100[012] ")
QUOTES=$(grep -c " QUOTE " "$TMP/tail.log")
[ "$DATA" -gt 1000 ] || fail "expected >1000 data records on the ring, got $DATA"
[ "$HB" -ge 2 ] || fail "expected heartbeats on the ring, got $HB"
[ "$BAD" -eq 0 ] || fail "$BAD records for instruments outside the filter"
[ "$QUOTES" -eq 0 ] || fail "$QUOTES QUOTE records despite the msg_types filter"
echo "ring ok: data=$DATA heartbeats=$HB"
kill $AGENT_PID; wait $AGENT_PID 2>/dev/null
echo "PASS"
