# The toolchain image (INV-007): Ubuntu 26.04, GCC 16. Built and run by scripts/dev.sh;
# CI runs scripts/deps-2604.sh on the same pinned base.
FROM ubuntu:26.04@sha256:6a06111c3b0a8600feadb270aec771dc239bf407b25625f93f44ab00e11f4d3c
COPY scripts/deps-2604.sh /tmp/deps.sh
RUN /tmp/deps.sh && rm /tmp/deps.sh && rm -rf /var/lib/apt/lists/*
