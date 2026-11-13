#!/usr/bin/env bash
# subplane-lib against a real valkey-server: the store tests, then several client
# processes sharing it. Skipped (77) when valkey-server is not installed.
# usage: valkey_e2e.sh <test_lib_valkey> <lib_client>
set -u
TEST=$1; CLIENT=$2
command -v valkey-server >/dev/null && command -v valkey-cli >/dev/null || { echo "SKIP: no valkey-server/valkey-cli"; exit 77; }
TMP=$(mktemp -d); trap 'kill $(jobs -p) 2>/dev/null; wait 2>/dev/null; rm -rf "$TMP"' EXIT
fail() { echo "FAIL: $*"; for f in "$TMP"/*.log; do [ -f "$f" ] && { echo "--- $f"; tail -15 "$f"; }; done; exit 1; }
PORT=$(( 20000 + (RANDOM % 30000) ))
start_valkey() {
  valkey-server --port "$PORT" --bind 127.0.0.1 --save '' --appendonly no --dir "$TMP" > "$TMP/valkey.log" 2>&1 &
  VALKEY_PID=$!
  for _ in $(seq 1 50); do valkey-cli -p "$PORT" ping 2>/dev/null | grep -q PONG && return 0; sleep 0.1; done
  fail "valkey-server did not start"
}
start_valkey

# 1. The store, through its scripts.
SUBPLANE_VALKEY_PORT=$PORT "$TEST" > "$TMP/store.log" 2>&1 || fail "store tests"
tail -2 "$TMP/store.log" | head -1

# 2. Four processes under one site bucket of 20000/s for 2 s: together they get
# the site rate (plus its burst and their leases), however fast each could go.
PIDS=()
for i in 1 2 3 4; do "$CLIENT" rate "$PORT" "{rate}:" 20000 20000 200 1000 2 > "$TMP/rate$i.log" 2>&1 & PIDS+=($!); done
for p in "${PIDS[@]}"; do wait "$p" || fail "a rate client failed"; done
TOTAL=0
for i in 1 2 3 4; do
  S=$(grep -o 'spent=[0-9]*' "$TMP/rate$i.log" | cut -d= -f2)
  [ -n "$S" ] || fail "rate client $i printed nothing"
  TOTAL=$(( TOTAL + S ))
done
echo "rate: 4 clients spent $TOTAL in 2 s under a site rate of 20000/s"
[ "$TOTAL" -ge 36000 ] && [ "$TOTAL" -le 44000 ] || fail "4 clients spent $TOTAL, want about 40000 (site 20000/s, burst 2000, leases 4 x 200)"

# 3. Failover: A is primary; B takes over when A is killed, with a higher epoch.
"$CLIENT" elect "$PORT" "{elect}:" gw.test 1000 6 > "$TMP/a.log" 2>&1 & A=$!
for _ in $(seq 1 50); do grep -q " elected epoch=1" "$TMP/a.log" && break; sleep 0.1; done
grep -q " elected epoch=1" "$TMP/a.log" || fail "A was not elected"
"$CLIENT" elect "$PORT" "{elect}:" gw.test 1000 6 > "$TMP/b.log" 2>&1 & B=$!
sleep 0.5
grep -q elected "$TMP/b.log" && fail "B was elected while A held the group"
kill -9 $A; KILLED=$(date +%s%N)
for _ in $(seq 1 30); do grep -q " elected epoch=2" "$TMP/b.log" && break; sleep 0.1; done
grep -q " elected epoch=2" "$TMP/b.log" || fail "B did not take over within 3 s"
TOOK=$(( ($(date +%s%N) - KILLED) / 1000000 ))
echo "elect: A killed, B primary at epoch 2 within ${TOOK} ms (ttl 1000 ms)"
kill $B 2>/dev/null; wait $B 2>/dev/null

# 4. Valkey goes away: the rate client drops to its floor rate; the primary steps down.
"$CLIENT" rate "$PORT" "{out}:" 20000 20000 200 1000 3 > "$TMP/floor.log" 2>&1 & R=$!
"$CLIENT" elect "$PORT" "{out}:" gw.out 1000 3 > "$TMP/c.log" 2>&1 & C=$!
for _ in $(seq 1 50); do grep -q " elected" "$TMP/c.log" && break; sleep 0.1; done
grep -q " elected" "$TMP/c.log" || fail "C was not elected"
sleep 0.5
kill -9 $VALKEY_PID; wait $VALKEY_PID 2>/dev/null
wait $R || fail "the rate client failed"
wait $C || fail "the elect client failed"
F=$(grep -o 'floor_tokens=[0-9]*' "$TMP/floor.log" | cut -d= -f2)
echo "outage: $(cat "$TMP/floor.log"); $(grep -E 'expired|lost' "$TMP/c.log")"
[ "${F:-0}" -gt 0 ] || fail "the rate client did not fall back to its floor rate"
[ "$F" -le $(( 1000 * 3 + 200 )) ] || fail "floor tokens $F exceed the floor rate (1000/s for under 3 s)"
grep -q " expired " "$TMP/c.log" || fail "the primary did not step down when the store went away"
echo "PASS"
