#!/usr/bin/env bash
# Every reference deployment config (deploy/*/agent.yaml) starts an agent that
# opens its partitions and sessions. Only what would collide on a shared host
# is changed: API port, socket and Aeron paths, ring size (CI's /dev/shm is 64 MiB).
# usage: test_reference_configs.sh <subplaned> <repo root>
set -u
AGENT=$1; ROOT=$2
TMP=$(mktemp -d); trap 'rm -rf "$TMP"; kill $(jobs -p) 2>/dev/null; rm -f /dev/shm/subplane.reftest*.ring' EXIT
http_get() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'GET %s HTTP/1.0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
n=0
for cfg in "$ROOT"/deploy/*/agent.yaml; do
  name=$(basename "$(dirname "$cfg")")
  API=$(( 20000 + (RANDOM % 30000) ))
  dir=$(dirname "$cfg")
  sed -e "s|schema: \([^/][^ ]*\)|schema: $dir/\1|" \
      -e "s|port: 8080|port: $API|" \
      -e "s|^  name: .*|  name: reftest$name|" \
      -e "s|socket_path: [^,]*,|socket_path: $TMP/$name.sock,|" -e "s|fallback_socket_path: [^,]*,|fallback_socket_path: $TMP/$name.sock,|" \
      -e "s|aeron_dir: \"\"|aeron_dir: \"$TMP/aeron-$name\"|" \
      -e "s|capacity_mb: [0-9]*|capacity_mb: 1|" "$cfg" > "$TMP/$name.yaml"
  "$AGENT" --config "$TMP/$name.yaml" > "$TMP/$name.log" 2>&1 & PID=$!
  P=""
  for _ in $(seq 1 100); do P=$(http_get "$API" /partitions 2>/dev/null) && [ -n "$P" ] && break; sleep 0.1; done
  kill -0 $PID 2>/dev/null || { echo "FAIL: $name: agent exited"; tail -20 "$TMP/$name.log"; exit 1; }
  S=$(http_get "$API" /sessions)
  case "$P" in *'"name"'*) ;; *) echo "FAIL: $name: no partitions: $P"; tail -20 "$TMP/$name.log"; exit 1 ;; esac
  case "$S" in *'"ring_path"'*) ;; *) echo "FAIL: $name: no sessions: $S"; tail -20 "$TMP/$name.log"; exit 1 ;; esac
  kill $PID; wait $PID 2>/dev/null
  echo "ok: deploy/$name"
  n=$((n + 1))
done
[ "$n" -gt 0 ] || { echo "FAIL: no reference configs found"; exit 1; }
echo "PASS: $n reference config(s)"
