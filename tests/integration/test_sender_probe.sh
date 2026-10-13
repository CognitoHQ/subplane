#!/usr/bin/env bash
# Sender -> probe round trip over unicast loopback for two framings.
# usage: test_sender_probe.sh <subplane send> <subplane probe> <schema.xml>
set -u
SP=$1; SCHEMA=$2
TMP=$(mktemp -d); trap 'rm -rf "$TMP"; kill $(jobs -p) 2>/dev/null' EXIT

BARE='{sequence: message.SeqNum}'
MOLD='{packet_header: [{name: session, type: bytes, size: 10}, {name: seq, type: u64be}, {name: count, type: u16be}], message_prefix: [{name: len, type: u16be}], sequence: packet_header.seq, count: packet_header.count, length: message_prefix.len}'

fail() { echo "FAIL: $*"; exit 1; }

run_case() {
    local name=$1 framing=$2 port out
    port=$(( 20000 + (RANDOM % 30000) ))
    out="$TMP/$name.probe"
    # Probe first: unicast datagrams sent before the socket is bound would be lost.
    "$SP" probe --schema "$SCHEMA" --framing "$framing" --port "$port" --packets 200 --timeout-ms 10000 --verbose >"$out" 2>"$TMP/$name.probe.err" &
    local ppid=$!
    sleep 0.5
    "$SP" send --schema "$SCHEMA" --framing "$framing" --port "$port" --count 200 --rate 2000 \
        --per-packet 3 --gap-every 50 --heartbeat-ms 10 >"$TMP/$name.sender" 2>&1 || fail "$name: sender failed: $(cat "$TMP/$name.sender")"
    wait "$ppid"; local rc=$?
    cat "$out"
    [ "$rc" -eq 0 ] || fail "$name: probe exit $rc"
    grep -Eq '^packets:[[:space:]]+200$' "$out" || fail "$name: expected 200 packets"
    local gaps
    gaps=$(sed -n 's/^sequence gaps: \([0-9]*\).*/\1/p' "$out")
    [ -n "$gaps" ] && [ "$gaps" -ge 3 ] || fail "$name: expected >= 3 sequence gaps, got '${gaps}'"
    echo "ok: $name (gaps=$gaps)"
}

# Bare concatenation (length from schema walk); sequence rides in the SeqNum message field
# because a bare bus has no packet header.
run_case bare "$BARE"
run_case mold "$MOLD"
echo "PASS"
