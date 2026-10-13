#!/usr/bin/env bash
# Build dependencies on Ubuntu 22.04 (GCC 11), shared by scripts/ci-build-2204.sh and GitHub CI.
# Aeron (cmake/aeron.cmake) needs CMake >= 3.30; jammy's apt ships 3.22, so CMake comes from Kitware.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq >/dev/null
apt-get install -y -qq g++ pkg-config libyaml-cpp-dev git ca-certificates gpg wget uuid-dev >/dev/null
wget -qO- https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor -o /usr/share/keyrings/kitware-archive-keyring.gpg
echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ jammy main' > /etc/apt/sources.list.d/kitware.list
apt-get update -qq >/dev/null
apt-get install -y -qq cmake >/dev/null
g++ --version | head -1
cmake --version | head -1
