#!/usr/bin/env bash
# Real-bus check: replay public CME MDP 3.0 packets through subplane probe with the
# MDP 3.0 framing block. Skips (exit 77) when the sample has not been fetched
# (scripts/fetch-cme-sample.sh needs network).
# usage: test_cme_probe.sh <subplane replay> <subplane probe> <sample-dir>
set -u
SP=$1; DIR=$2
[ -f "$DIR/cme_all.pcap" ] || { echo "SKIP: no CME sample in $DIR (run scripts/fetch-cme-sample.sh)"; exit 77; }
TMP=$(mktemp -d); trap 'rm -rf "$TMP"; kill $(jobs -p) 2>/dev/null' EXIT
fail() { echo "FAIL: $*"; exit 1; }
FRAMING=$(cat "$DIR/framing.yaml")

probe() { # name schema expected-exit
    local port out
    port=$(( 20000 + (RANDOM % 30000) )); out="$TMP/$1.probe"
    "$SP" probe --schema "$2" --framing "$FRAMING" --port "$port" --packets 27 --timeout-ms 10000 >"$out" 2>&1 &
    local ppid=$!; sleep 0.5
    "$SP" replay --pcap "$DIR/cme_all.pcap" --port "$port" --rate 500 >/dev/null 2>&1 || fail "$1: replay failed"
    wait "$ppid"; local rc=$?
    cat "$out"
    [ "$rc" -eq "$3" ] || fail "$1: probe exit $rc, expected $3"
    grep -Eq '^packets:[[:space:]]+27$' "$out" || fail "$1: expected 27 packets"
    grep -Eq '^messages:[[:space:]]+31$' "$out" || fail "$1: expected 31 messages"
    grep -Eq '^length mismatches:[[:space:]]+0$' "$out" || fail "$1: expected 0 length mismatches"
    grep -Eq '^truncated packets:[[:space:]]+0$' "$out" || fail "$1: expected 0 truncated packets"
}

# Wire versions 6 and 9 against the v9 schema: every template known, every length tiles.
# This covers multi-entry groups, groupSize8Byte and a sinceVersion=7 group absent from v6 packets.
probe v9 "$DIR/templates_v9.xml" 0
# v13 schema dropped templates 32 and 35: they must be reported unknown, nothing else may change.
probe v13 "$DIR/templates_v13.xml" 1
grep -Eq '^unknown template ids:[[:space:]]+32 35 \(9 messages\)' "$TMP/v13.probe" || fail "v13: expected templates 32 and 35 unknown"
echo "PASS"
