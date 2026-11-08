# Contributing

subplane is infrastructure: correctness and stability over velocity. Read
`docs/context/invariants.md` before changing anything on the hot path.

## Ground rules

- **Hot path**: no heap, no mutex, no I/O, no `std::function`, at most one
  virtual call per packet batch. Measure before optimizing; `subplane replay --bench`
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

1. Build and test: `scripts/dev.sh test` (Docker). It builds `./build` inside the toolchain image.
2. The toolchain is GCC 16 on Ubuntu 26.04 with `-std=c++26` and `-Werror` (INV-007), in
   `docker/Dockerfile`; CI runs `scripts/deps-2604.sh` on the same pinned base on every push.
   CMake refuses an older GCC, so a host build fails early instead of differing quietly.
3. Real-packet check: `scripts/fetch-cme-sample.sh` then `ctest -R cme_probe`.
4. One logical change per commit; explain the why in the message body.
5. Open a pull request against `main`. CI must be green.
6. A change to a contract path (`.github/compat-paths.txt`) adds a row to
   `docs/compat-log.md`, or the PR says `Compat: none (<reason>)`; CI checks it.

## Reporting

- **Bugs and features**: open an issue from the templates.
- **Security**: privately, never in a public issue; see `SECURITY.md`.
- **Conduct**: see `CODE_OF_CONDUCT.md`.

## Licence and CLA

subplane is source-available under the Functional Source License 1.1 with an
Apache-2.0 future licence (FSL-1.1-ALv2, see `LICENSE`): use it for anything
except a competing commercial product or service, and each release becomes
Apache-2.0 two years after it ships. The app contract (paths in `NOTICE`) is
Apache-2.0 now. Before a first contribution is merged, its author signs the Individual
Contributor License Agreement (`CLA.md`) by adding a row to
`CLA-signatures.md` in their pull request; CI checks that the author of every
pull request has one. If your employer has rights to what you write, it signs
the Corporate CLA (`CLA-corporate.md`) first.
