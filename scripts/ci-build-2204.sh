#!/usr/bin/env bash
# Build and test flashtick inside ubuntu:22.04 (GCC 11), the INV-007 target toolchain.
# Usage: scripts/ci-build-2204.sh            (from the repo root)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
docker run --rm -v "$ROOT":/src:ro -w /src ubuntu:22.04 bash -euo pipefail -c '
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq >/dev/null
  apt-get install -y -qq g++ cmake pkg-config libyaml-cpp-dev >/dev/null
  g++ --version | head -1
  cmake -S /src -B /tmp/b -DCMAKE_BUILD_TYPE=Release >/dev/null
  cmake --build /tmp/b -j"$(nproc)"
  cd /tmp/b && ctest --output-on-failure
'
