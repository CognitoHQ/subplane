#!/usr/bin/env bash
# Build dependencies on Ubuntu 26.04 with GCC 16 (INV-007), shared by docker/Dockerfile and GitHub CI.
# g++-16 is Ubuntu's package (26.04's own libstdc++ comes from the same GCC 16 source), made the
# default g++ so CMake and every script pick it up.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq >/dev/null
apt-get install -y -qq gcc-16 g++-16 cmake make pkg-config libyaml-cpp-dev git ca-certificates uuid-dev python3 python3-numpy >/dev/null
for t in gcc g++ cc c++; do
  case $t in gcc|cc) v=/usr/bin/gcc-16 ;; *) v=/usr/bin/g++-16 ;; esac
  update-alternatives --install /usr/bin/$t $t $v 100 >/dev/null
done
g++ --version | head -1
cmake --version | head -1
