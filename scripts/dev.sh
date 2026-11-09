#!/usr/bin/env bash
# Build, test and run inside the toolchain image (docker/Dockerfile: Ubuntu 26.04, GCC 16; invariant INV-007).
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
# Every run shares the host's network (no bridge on UDP/multicast paths), /dev/shm (rings, Aeron IPC)
# and /tmp (control sockets, configs), so processes in separate dev.sh runs, one per terminal, see
# each other as they would on the host. Fixed paths there are shared too: run one test suite at a time.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SHARE="$(dirname "$ROOT")"
TAG="subplane-dev:$(cat "$ROOT/docker/Dockerfile" "$ROOT/scripts/deps-2604.sh" | sha256sum | cut -c1-12)"
docker image inspect "$TAG" >/dev/null 2>&1 || docker build -q -t "$TAG" -f "$ROOT/docker/Dockerfile" "$ROOT" >/dev/null

# INTENT: without --ipc host and the /tmp mount each run gets a private /dev/shm and /tmp, so a ring or a
# control socket made by the agent in one terminal does not exist for tail or an app in another.
mkdir -p /tmp/subplane-dev-home
opts=(--rm --init --network host --ipc host -v /tmp:/tmp -v "$SHARE:$SHARE" -w "$ROOT" -e HOME=/tmp/subplane-dev-home)
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
