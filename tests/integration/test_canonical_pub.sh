#!/usr/bin/env bash
# The sample publisher (examples/canonical_pub.cpp) -> subplaned -> ring -> views_app.
# usage: test_canonical_pub.sh <canonical_pub> <subplaned> <views_app> <schema.xml>
set -u
PUB=$1; AGENT=$2; APP=$3; SCHEMA=$4
TMP=$(mktemp -d); trap 'rm -rf "$TMP"; kill $(jobs -p) 2>/dev/null; rm -f /dev/shm/subplane.pubtest.*.ring' EXIT
fail() { echo "FAIL: $*"; tail -30 "$TMP"/*.log 2>/dev/null; exit 1; }
http_get() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'GET %s HTTP/1.0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
http_post() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'POST %s HTTP/1.0\r\nContent-Length: 0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
PORT=$(( 20000 + (RANDOM % 30000) )); API=$(( 20000 + (RANDOM % 30000) )); SOCK="$TMP/ctl.sock"
cat > "$TMP/config.yaml" <<YAML
agent: { name: pubtest, version: test, max_symbols: 65536, thread_groups: [ { name: g0, cpu: -1, topics: [can0] } ] }
registry: [ { id: 1, name: can0 } ]
topics:
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
  - { name: all, ring: { capacity_mb: 8, max_payload: 4096 }, topics: [can0], symbols: ["*"], msg_types: [], deliver: { bus_admin: true, agent_control: true } }
fanout: { ring_dir: /dev/shm }
control: { socket_path: $SOCK, fallback_socket_path: $SOCK, linger_ms: 1000 }
lvc: { max_payload: 512 }
api: { enabled: true, host: 127.0.0.1, port: $API }
metrics: { enabled: false, report_interval_seconds: 60, output_format: json, console_output: false }
observability: { site: lab, env: test, labels: { team: md } }
logging: { level: info, console_output: true, output_format: text }
YAML
"$AGENT" --config "$TMP/config.yaml" > "$TMP/agent.log" 2>&1 & AGENT_PID=$!
for _ in $(seq 1 100); do http_get "$API" /status >/dev/null 2>&1 && break; sleep 0.1; done
kill -0 $AGENT_PID 2>/dev/null || fail "agent exited early"
"$PUB" --port "$PORT" --count 1000 --rate 10000 > "$TMP/pub.log" 2>&1 || fail "publisher failed"
grep -q "sent 1000 messages in 500 packets" "$TMP/pub.log" || fail "publisher report: $(cat "$TMP/pub.log")"
RING=/dev/shm/subplane.pubtest.all.ring
for _ in $(seq 1 50); do [ -f "$RING" ] && break; sleep 0.1; done
# Latency tracing for every reader of the session, before the app opens the
# ring: its SDK claims a reader slot and applies the flag at once.
TR=$(http_post "$API" "/sessions/all/trace?on=1") || fail "POST trace"
case "$TR" in *'"tracing":true'*) ;; *) fail "trace reply: $TR" ;; esac
case "$(http_post "$API" "/sessions/nope/trace?on=1")" in *'no session nope'*) ;; *) fail "trace on a missing session" ;; esac
# A request split across TCP segments is read whole (the server used to answer
# the first segment and reset the connection).
SPLIT=$( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$API" || exit 1; printf 'POST /sessions/nope/tr' >&3; sleep 0.3
         printf 'ace?on=1 HTTP/1.0\r\nContent-Length: 0\r\n\r\n' >&3; tr -d '\r' <&3 | sed '1,/^$/d' )
case "$SPLIT" in *'no session nope'*) ;; *) fail "split request: '$SPLIT'" ;; esac
SUBPLANE_APP_NAME=views-test "$APP" "$RING" --from-start --quiet --seconds 3 > "$TMP/app.log" 2>&1 & APP_PID=$!
# /topology while views_app reads the ring: the agent finds it by its mapping
# of the ring file and by the reader slot its SDK claimed (docs/context/observability.md).
H=$(hostname | tr 'A-Z' 'a-z')
TOPO=""
for _ in $(seq 1 20); do
  sleep 0.25
  TOPO=$(http_get "$API" /topology) || continue
  case "$TOPO" in *'"tracing":true'*) break ;; esac
done
for want in '"schema":"subplane.topology/1"' \
  '"id":"'"$(hostname)"'/subplaned/pubtest"' \
  '"key":"udp://'"$H"':'"$PORT"'","role":"data"' \
  '"key":"shm-ring://'"$H"'/dev/shm/subplane.pubtest.all.ring","role":"data","state":"up","local":{"session":"all","owner":"config","topics":["can0"]}' \
  '"comm":"views_app","name":"views-test"' '"via":["mapping","slot"],"reader":{"slot":0,"lag_bytes":0' \
  '"tracing":true,"trace_requested":true,"sdk_version":1,"capabilities":1}'; do
  case "$TOPO" in *"$want"*) ;; *) fail "topology missing: $want in $TOPO" ;; esac
done
if command -v python3 >/dev/null; then printf '%s' "$TOPO" | python3 -m json.tool >/dev/null || fail "topology JSON invalid"; fi
http_get "$API" /metrics | grep -qF 'subplane_session_consumers{service="subplaned",instance="pubtest",host="'"$(hostname)"'",session="all"} 1' \
  || fail "consumers metric"
L='service="subplaned",instance="pubtest",host="'"$(hostname)"'",session="all",app="views-test",pid="'"$APP_PID"'"'
http_get "$API" /metrics > "$TMP/m1.txt"
for want in "subplane_app_lag_bytes{$L} 0" "subplane_app_tracing{$L} 1" "subplane_app_overruns_total{$L} 0" \
            "subplane_app_read_latency_seconds_bucket{$L,le=\"+Inf\"}" 'subplane_reader_slots_used{service="subplaned",instance="pubtest",host="'"$(hostname)"'"} 1'; do
  grep -qF -- "$want" "$TMP/m1.txt" || fail "metrics missing: $want"
done
N=$(sed -n 's/^subplane_app_read_latency_seconds_count{.*} \([0-9]*\)$/\1/p' "$TMP/m1.txt")
[ "${N:-0}" -ge 1000 ] || fail "traced records: ${N:-none}"
wait $APP_PID || fail "views_app exit $?"
# The reader released its slot on close.
for _ in $(seq 1 20); do http_get "$API" /sessions | grep -q '"readers":\[\]' && break; sleep 0.1; done
http_get "$API" /sessions | grep -q '"readers":\[\]' || fail "slot not released: $(http_get "$API" /sessions)"
SUMMARY=$(tail -1 "$TMP/app.log"); echo "$SUMMARY"
[ "$SUMMARY" = "trades=500 quotes=500 books=0 snapshots=0 other=0 heartbeats=0 bad=0" ] || fail "unexpected summary"
# /metrics: Prometheus text by default (docs/context/observability.md), JSON on request.
http_get "$API" /metrics > "$TMP/metrics.txt" || fail "GET /metrics"
for want in \
  '# TYPE subplane_partition_messages_total counter' \
  'subplane_partition_messages_total{service="subplaned",instance="pubtest",host="'"$(hostname)"'",topic="can0",partition="can0/1"} 1000' \
  'subplane_session_published_total{service="subplaned",instance="pubtest",host="'"$(hostname)"'",session="all"}' \
  '# TYPE subplane_recv_to_publish_seconds histogram' \
  'subplane_recv_to_publish_seconds_count{service="subplaned",instance="pubtest",host="'"$(hostname)"'",group="g0"} 500' \
  'subplane_service_info{service="subplaned",instance="pubtest",host="'"$(hostname)"'",version="test",epoch="' \
  'site="lab",env="test",team="md"} 1' ; do
  grep -qF -- "$want" "$TMP/metrics.txt" || fail "metrics missing: $want"
done
# Every family has HELP then TYPE, once.
[ "$(grep -c '^# TYPE ' "$TMP/metrics.txt")" = "$(grep -c '^# HELP ' "$TMP/metrics.txt")" ] || fail "HELP/TYPE mismatch"
[ -z "$(grep '^# TYPE ' "$TMP/metrics.txt" | awk '{print $3}' | sort | uniq -d)" ] || fail "a family is split"
# promtool lints the whole exposition when it is installed (always in the toolchain image and CI).
PROMTOOL=${PROMTOOL:-$(command -v promtool || true)}
if [ -n "$PROMTOOL" ]; then "$PROMTOOL" check metrics < "$TMP/metrics.txt" || fail "promtool check metrics"; fi
http_get "$API" '/metrics?format=json' > "$TMP/metrics.json" || fail "GET /metrics?format=json"
if command -v python3 >/dev/null; then python3 -m json.tool "$TMP/metrics.json" >/dev/null || fail "metrics JSON invalid"; fi
kill $AGENT_PID; wait $AGENT_PID 2>/dev/null
echo "PASS"
