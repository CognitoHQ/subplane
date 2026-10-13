# Contributing

subplane is infrastructure: correctness and stability over velocity. Read
`docs/context/invariants.md` before changing anything on the hot path.

## Ground rules

- **Hot path**: no heap, no mutex, no I/O, no `std::function`, at most one
  virtual call per packet batch. Measure before optimizing; `sp_replay --bench`
  is the harness.
- **Public contract**: `core/message.h`, `shm/ring_layout.h`,
  `shm/control_events.h` and `control/control_proto.h` are a versioned
  specification (INV-009, INV-016). Additive changes only within a version;
  anything else bumps the version and readers must refuse mismatches.
- **Transport-agnostic**: nothing an app can observe identifies the bus
  (INV-015). New transports map onto the existing control vocabulary.
- **`// INTENT:` comments** on lock-free code, ring and record layout, and
  component lifecycle: say why it exists and what breaks without it.
- **Docs mark implemented vs planned.** Every claim in `docs/context` is
  implemented or tagged "(planned)".

## Workflow

1. Build and test: `cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j && (cd build && ctest --output-on-failure)`.
2. The target toolchain is GCC 11 on Ubuntu 22.04 with `-Werror`;
   `scripts/ci-build-2204.sh` runs it in a container and CI runs it on every push.
3. Real-packet check: `scripts/fetch-cme-sample.sh` then `ctest -R cme_probe`.
4. One logical change per commit; explain the why in the message body.
5. Open a pull request against `main`. CI must be green.

## Licence

By contributing you agree that your contributions are licensed under the
Apache License 2.0 (see `LICENSE`).
