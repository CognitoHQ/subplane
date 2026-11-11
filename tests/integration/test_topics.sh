#!/usr/bin/env bash
# Topics end to end: a topic of two partitions and a topic of one, sessions by
# pattern, /topics and /partitions, data on partition 2 reaching its topic's
# sessions, /reload reporting the topic edits it did not apply, and the config
# errors that replace silent misconfiguration.
# usage: test_topics.sh <subplaned> <canonical_pub> <schema.xml>
set -u
AGENT=$1; PUB=$2; SCHEMA=$3
TMP=$(mktemp -d); trap 'rm -rf "$TMP"; kill $(jobs -p) 2>/dev/null; rm -f /dev/shm/subplane.topictest*.ring' EXIT
fail() { echo "FAIL: $*"; tail -30 "$TMP"/*.log 2>/dev/null; exit 1; }
http_get() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'GET %s HTTP/1.0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
http_post() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'POST %s HTTP/1.0\r\nContent-Length: 0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
P1=$(( 20000 + (RANDOM % 30000) )); P2=$((P1 + 1)); P3=$((P1 + 2)); API=$(( 20000 + (RANDOM % 30000) )); SOCK="$TMP/ctl.sock"

cat > "$TMP/registry.yaml" <<YAML
- { id: 21, name: x.a }
- { id: 22, name: x.b }
YAML
config() { # $1 = name, then the topics section on stdin
  cat > "$TMP/$1.yaml" <<YAML
agent: { name: topictest, version: test, max_symbols: 65536 }
registry: registry.yaml
topics:
$(cat)
sessions:
  - { name: s1, ring: { capacity_mb: 8, max_payload: 4096 }, topics: [x.a] }
  - { name: s2, ring: { capacity_mb: 8, max_payload: 4096 }, topics: ["x.>"] }
fanout: { ring_dir: /dev/shm }
control: { socket_path: $SOCK, fallback_socket_path: $SOCK, linger_ms: 1000 }
lvc: { max_payload: 512 }
api: { enabled: true, host: 127.0.0.1, port: $API }
metrics: { enabled: false, report_interval_seconds: 60, output_format: json, console_output: false }
logging: { level: info, console_output: true, output_format: text }
YAML
}
TOPIC_A="  - name: x.a
    transport: { type: udp, group: \"\", interface: \"\", rcvbuf: 4194304 }
    framing: {packet_header: [{name: seq, type: u64le}, {name: count, type: u16le}], message_prefix: [{name: len, type: u16le}], sequence: packet_header.seq, length: message_prefix.len}
    decoder:
      type: sbe_routing
      schema: $SCHEMA
      fields: { symbol: InstrumentId, sequence: Sequence, exchange_ts: ExchangeTs, venue: VenueId }
      messages: [ { template: 1, msg_type: TRADE }, { template: 2, msg_type: QUOTE }, { template: 100, admin: HEARTBEAT } ]
    heartbeat_timeout_ms: 2000"
config good <<YAML
$TOPIC_A
    partitions:
      - transport: { port: $P1 }
      - transport: { port: $P2 }
$(sed 's/name: x.a/name: x.b/' <<<"$TOPIC_A")
    partitions: [ { transport: { port: $P3 } } ]
YAML

"$AGENT" --config "$TMP/good.yaml" > "$TMP/agent.log" 2>&1 & AGENT_PID=$!
for _ in $(seq 1 100); do http_get "$API" /status >/dev/null 2>&1 && break; sleep 0.1; done
kill -0 $AGENT_PID 2>/dev/null || fail "agent exited early"

T=$(http_get "$API" /topics)
grep -q '"name":"x.a","topic_id":21,"degraded":0,"partitions":\[{"partition_num":1,"stream_id":1376257,"state":"init"},{"partition_num":2,"stream_id":1376258,"state":"init"}\],"sessions":\["s1","s2"\]' <<<"$T" || fail "/topics x.a: $T"
grep -q '"name":"x.b","topic_id":22,.*"sessions":\["s2"\]' <<<"$T" || fail "/topics x.b: $T"
P=$(http_get "$API" /partitions)
grep -q '"name":"x.a/2","topic":"x.a","topic_id":21,"partition_num":2,"stream_id":1376258' <<<"$P" || fail "/partitions: $P"
grep -q '"topics":\["x.>"\]' <<<"$(http_get "$API" /sessions)" || fail "/sessions topics"
echo "ok: /topics, /partitions, /sessions"

# Data on partition 2 reaches both sessions; partition 1 sees none of it.
"$PUB" --port "$P2" --count 1000 --rate 10000 > "$TMP/pub.log" 2>&1 || fail "publisher failed"
for _ in $(seq 1 50); do P=$(http_get "$API" /partitions); grep -q '"name":"x.a/2".*"messages":1000' <<<"$P" && break; sleep 0.1; done
grep -q '"name":"x.a/2","topic":"x.a","topic_id":21,"partition_num":2,"stream_id":1376258,"state":"active","last_sequence":[0-9]*,"packets":500,"messages":1000' <<<"$P" || fail "partition 2 counts: $P"
grep -q '"name":"x.a/1",[^}]*"packets":0' <<<"$P" || fail "partition 1 got data: $P"
S=$(http_get "$API" /sessions)
for s in s1 s2; do grep -q "\"name\":\"$s\",[^]]*\"published\":1000," <<<"$S" || fail "session $s did not get 1000: $S"; done
echo "ok: partition 2 -> sessions s1 and s2"

# An edit to topics is reported by /reload, not silently dropped.
sed -i "s/port: $P3/port: $((P3 + 10))/" "$TMP/good.yaml"
R=$(http_post "$API" /reload)
grep -q '"ignored":\["topics"\]' <<<"$R" || fail "/reload did not report the topics edit: $R"
echo "ok: /reload ignored: topics"
kill $AGENT_PID; wait $AGENT_PID 2>/dev/null

# Misconfigurations the agent refuses, each with a clear error.
refuse() { # $1 = name, $2 = expected text; config in $TMP/$1.yaml
  "$AGENT" --config "$TMP/$1.yaml" > "$TMP/$1.log" 2>&1 & local pid=$!
  for _ in $(seq 1 50); do kill -0 $pid 2>/dev/null || break; sleep 0.1; done
  if kill -0 $pid 2>/dev/null; then kill $pid; fail "$1: agent started"; fi
  grep -q -- "$2" "$TMP/$1.log" || fail "$1: expected '$2': $(tail -3 "$TMP/$1.log")"
  echo "ok: refused $1"
}
sed 's/^topics:/partitions:/' "$TMP/good.yaml" > "$TMP/old.yaml";             refuse old "is replaced by \`topics:\`"
sed 's/name: x.b/name: x.c/' "$TMP/good.yaml" > "$TMP/unregistered.yaml";    refuse unregistered "topic x.c is not in the topic registry"
sed "s/port: $((P3 + 10))/port: $P2/" "$TMP/good.yaml" > "$TMP/samestream.yaml"; refuse samestream "read the same stream"
sed 's/^registry: .*/registry: [ { id: 21, name: x.a }, { id: 21, name: x.b } ]/' "$TMP/good.yaml" > "$TMP/dupid.yaml"; refuse dupid "topic id 21 is given to both"
sed 's/max_symbols: 65536 }/max_symbols: 65536, thread_groups: [ { name: g0, topics: [x.a] }, { name: g1, topics: [x.a, x.b] } ] }/' "$TMP/good.yaml" > "$TMP/twogroups.yaml"; refuse twogroups "topic x.a is in more than one thread group"
# Session errors are logged and the agent runs on (operators fix the file and reload).
logged() { # $1 = name, $2 = expected text
  "$AGENT" --config "$TMP/$1.yaml" > "$TMP/$1.log" 2>&1 & local pid=$!
  for _ in $(seq 1 50); do grep -q -- "$2" "$TMP/$1.log" && break; sleep 0.1; done
  kill -0 $pid 2>/dev/null || fail "$1: agent exited"
  kill $pid; wait $pid 2>/dev/null
  grep -q -- "$2" "$TMP/$1.log" || fail "$1: expected '$2': $(tail -3 "$TMP/$1.log")"
  echo "ok: logged $1"
}
sed 's/topics: \[x.a\] }/topics: [x.z] }/' "$TMP/good.yaml" > "$TMP/nomatch.yaml";     logged nomatch "topic pattern x.z matches no topic"
sed 's/topics: \[x.a\] }/partitions: [x.a] }/' "$TMP/good.yaml" > "$TMP/sessparts.yaml"; logged sessparts "subscribe by \`topics:\`"
echo "PASS: topics"
