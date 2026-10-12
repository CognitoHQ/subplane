#!/usr/bin/env bash
# ft_replay --bench smoke test: runs the in-process pipeline benchmark against a temp copy of
# config.yaml whose schema path points at the sample schema; checks the JSON report.
# usage: test_replay_bench.sh <ft_replay> <config.yaml> <schema.xml>
set -u
REPLAY=$1; CONFIG=$2; SCHEMA=$3
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
fail() { echo "FAIL: $*"; exit 1; }

sed "s#^\\([[:space:]]*schema:\\).*#\\1 $SCHEMA#" "$CONFIG" > "$TMP/config.yaml"
grep -q "schema: $SCHEMA" "$TMP/config.yaml" || fail "schema path rewrite did not apply"

"$REPLAY" --bench --config "$TMP/config.yaml" --packets 20000 --sessions 2 --json > "$TMP/raw.txt" 2> "$TMP/err.txt" \
    || fail "ft_replay exit $?: $(cat "$TMP/err.txt")"
grep '^{' "$TMP/raw.txt" > "$TMP/out.json" || fail "no JSON line in output"
cat "$TMP/out.json"
# Assertions in plain bash (CI containers may not have python3).
num() { # num <json file> <section> <key>  -> integer value of "key" inside the section object
    sed -n "s/.*\"$2\":{\([^}]*\(recv_to_publish\":{[^}]*}\)\?[^}]*\)}.*/\1/p" "$1" | grep -o "\"$3\":[0-9]*" | head -1 | cut -d: -f2
}
p50() { sed -n 's/.*"with_sessions":{.*"recv_to_publish":{[^}]*"p50":\([0-9]*\).*/\1/p' "$1"; }
S=$(num "$TMP/out.json" with_sessions sessions); M=$(num "$TMP/out.json" with_sessions messages)
P=$(num "$TMP/out.json" with_sessions published); B=$(num "$TMP/out.json" poll_only messages); Q=$(p50 "$TMP/out.json")
[ "${S:-0}" = "2" ] || fail "sessions=$S"
[ "${M:-0}" -gt 0 ] || fail "messages=$M"
[ "${P:-0}" -gt 0 ] || fail "published=$P"
[ "${Q:-0}" -gt 0 ] || fail "p50=$Q"
[ "${B:-0}" -gt 0 ] || fail "poll_only messages=$B"
echo "ok: replay_bench"
