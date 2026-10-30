#!/usr/bin/env bash
# Build, test and run inside the toolchain image (docker/Dockerfile: Ubuntu 26.04, GCC 16, INV-007).
# The host's compiler is never used. The parent directory is mounted at its own path, so sibling
# checkouts (subplane-feeds) and absolute paths in build directories work on both sides.
#
#   scripts/dev.sh build                 configure and build ./build (Release)
#   scripts/dev.sh test [ctest args]     build, then ctest --output-on-failure
#   scripts/dev.sh run <cmd...>          any command in the image, as you
#   scripts/dev.sh bench <cmd...>        as root with SYS_NICE and no seccomp, for pinned
#                                        SCHED_FIFO runs (host: sched_rt_runtime_us=-1)
#   scripts/dev.sh shell                 interactive shell
#
# Every run: --network host (no bridge on UDP/multicast paths), --shm-size=2g (Aeron IPC, rings).
# PROMTOOL=/path/to/promtool is passed through and its directory mounted read-only.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SHARE="$(dirname "$ROOT")"
TAG="subplane-dev:$(cat "$ROOT/docker/Dockerfile" "$ROOT/scripts/deps-2604.sh" | sha256sum | cut -c1-12)"
docker image inspect "$TAG" >/dev/null 2>&1 || docker build -q -t "$TAG" -f "$ROOT/docker/Dockerfile" "$ROOT" >/dev/null

opts=(--rm --init --network host --shm-size=2g -v "$SHARE:$SHARE" -w "$ROOT" -e HOME=/tmp)
if [ -n "${PROMTOOL:-}" ]; then opts+=(-v "$(dirname "$PROMTOOL"):$(dirname "$PROMTOOL"):ro" -e "PROMTOOL=$PROMTOOL"); fi
[ -t 0 ] && [ -t 1 ] && opts+=(-it)
as_me=(--user "$(id -u):$(id -g)" -e USER="$(id -un)")
build='cmake -S . -B build -DCMAKE_BUILD_TYPE=Release ${CMAKE_ARGS:-} >/dev/null && cmake --build build -j"$(nproc)"'

cmd=${1:-}; [ $# -gt 0 ] && shift
case "$cmd" in
  build) exec docker run "${opts[@]}" "${as_me[@]}" -e CMAKE_ARGS="${CMAKE_ARGS:-}" "$TAG" bash -c "$build" ;;
  test)  exec docker run "${opts[@]}" "${as_me[@]}" -e CMAKE_ARGS="${CMAKE_ARGS:-}" "$TAG" bash -c "$build"' && cd build && ctest --output-on-failure "$@"' bash "$@" ;;
  run)   exec docker run "${opts[@]}" "${as_me[@]}" "$TAG" "$@" ;;
  bench) exec docker run "${opts[@]}" --cap-add SYS_NICE --security-opt seccomp=unconfined "$TAG" "$@" ;;
  shell) exec docker run "${opts[@]}" "${as_me[@]}" "$TAG" bash ;;
  *) sed -n '2,15p' "$0"; exit 2 ;;
esac
