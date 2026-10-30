# subplane: Non-Negotiable Requirements

These are hard rules. Every change must honour all of them. Numbers are stable: a retired number is not reused.

---

## INV-001: Retired

Was "Plugin ABI stability". Retired because subplane no longer loads plugins: it is a single static binary with compiled-in components selected by YAML (no `dlopen`, no `.so`). The ABI that matters now is the ring and record layout, covered by INV-009.

---

## INV-002: The pipeline thread never blocks

The pipeline thread (the per-group busy-polling loop: poll, frame, decode, sequence check, last-value update, filter, ring publish) must not block, allocate heap memory, take a lock, or perform I/O beyond the transport poll. One indirect call per packet batch (`Transport::poll`) is allowed. Cold-side work reaches the thread only through the SPSC command queue.

Known exception to track: symbol resolution during decode is a virtual call per message (`SymbolResolver`), marked TODO in `decoder/sbe_routing_decoder.h`. It does not block or allocate, but it is outside the one-call-per-batch rule until it is replaced by a direct call.

**Rationale**: a single mutex, allocation or syscall on the hot path destroys latency guarantees and makes percentile metrics meaningless.

---

## INV-003: No hardcoded credentials

All secrets (API keys, SASL credentials, SSL key paths, tokens) must come from configuration files or environment variables. No key, password, or token may appear as a string literal in source code.

**Rationale**: keys in code get leaked. One commit is enough.

---

## INV-004: Failures stay away from the writer

A partition, publisher, or reader failure never affects the writer or the other sessions. Concretely:

- **Unknown template.** Counted (`unknown_templates`) and the message skipped when its length is known; when the length cannot be computed, the rest of the packet is dropped and counted (`undecodable_tail`). Never guessed, never propagated as corrupt data.
- **Malformed, truncated, wrong-schema data** is counted (`malformed`, `truncated`, `decode_errors`, `schema_mismatch`) and skipped.
- **Gaps are never silent.** A forward sequence jump emits an in-band `GAP_DETECTED` event and increments `gaps`. Duplicates are counted and dropped.
- **Publisher restarts.** A backward sequence jump larger than `dup_window` is treated as a restart (`seq_resets`, in-band `GAP_DETECTED` with `received < expected`), and a `SEQ_RESET` admin message resets the expected sequence. A silent publisher degrades the partition after `heartbeat_timeout_ms` (`PARTITION_DEGRADED`) and it recovers on the next packet.
- **Slow or dead readers.** Readers map rings read-only and never delay the writer. A lapped reader detects it itself and resyncs. A reader that disconnects or crashes costs the writer nothing.
- **Oversize payload.** A record larger than the session's `max_payload` is not published and is counted (`publish_failed`); other sessions are unaffected.
- **Transport errors** are counted (`transport_errors`) and the loop continues.
- **Startup is fail-fast for configuration** (unknown component type, bad schema, bad framing), but a failed session or control-socket setup is logged and the agent keeps running.

**Rationale**: production feeds deliver malformed data, restart, and fall silent, and apps crash. The agent must stay up and its output must stay truthful about what it dropped.

---

## INV-005: Thread safety on all public APIs

Every public method on Agent, SessionManager, ControlServer, MetricsManager, and ApiServer must be safe to call from any thread, or be clearly marked otherwise in its header (for example, `Pipeline::run_once` and the hot-side state of a `Partition` belong to the pipeline thread). Document thread-safety guarantees in the header. If a method is not thread-safe, it must be private or clearly marked.

**Rationale**: the agent runs multiple threads (pipeline groups, control, API, metrics). Unmarked thread-unsafe public APIs are latent data races.

---

## INV-006: Records must carry timestamps

Every record delivered to a session has a non-zero `ingest_ts`, set at packet receive time (kernel timestamp when enabled, otherwise right after `recvmmsg`). No record may be published to a ring or returned in a snapshot without one.

**Rationale**: consumers rely on ingest time for ordering, latency measurement, and staleness detection. A zero timestamp is indistinguishable from "time unknown".

---

## INV-007: One toolchain: C++26, GCC 16, Ubuntu 26.04; the app SDK stays C++20

The agent and tools compile with `-std=c++26` and `-Werror` under GCC 16 on Ubuntu 26.04, in one pinned image (`docker/Dockerfile`, run by `scripts/dev.sh`); CI runs `scripts/deps-2604.sh` on the same pinned base. CMake refuses GCC older than 16. The host compiler is never used.

The app SDK (`include/subplane/client/`, `control/control_proto.h`, `shm/`, `core/message.h`, the generated views) is compiled by apps with their own toolchains, so it must stay C++20: the example apps build as C++20 and fail the build if it does not.

**Rationale**: one compiler everywhere, so a local build proves what CI checks (with GCC 11 in CI and a newer host compiler, it did not). C++26 for `std::inplace_vector` and `std::simd`. GCC 16 is Ubuntu 26.04's `g++-16`, a snapshot of GCC 16 before 16.1 whose libstdc++ is 26.04's own runtime; GCC marks C++26 experimental, so its ABI may change between GCC releases, which only matters across a shared-library boundary, and the agent has none in C++.

---

## INV-008: Config changes must not require recompilation

Adding a bus partition, changing its group, port, framing or schema mapping, adding or changing sessions, pinning threads, or enabling and disabling the API must be achievable by editing `config.yaml` alone. If a feature requires a code change to configure, it is missing a config path. (Adding a new kind of component, such as a new transport, is a code change by design.)

**Rationale**: operational changes should not require a build cycle. The activation model is pointless if you have to recompile to use it.

---

## INV-009: Ring and record layout is versioned

`RING_LAYOUT_VERSION` (`shm/ring_layout.h`) covers the ring header and framing rules. `RECORD_VERSION` (`core/message.h`) covers `TickHeader` and every control event payload in `shm/control_events.h`. Any layout change bumps the matching version. The writer stamps both into the ring header; readers (`ShmReader::open`) refuse a ring whose magic or either version differs. Layouts are frozen by `static_assert`s on size and offsets.

**Rationale**: the ring is read by foreign processes built at different times. A silent layout drift corrupts every reader.

---

## INV-010: Control events are in-band and ordered with data

Gap, degraded, recovered, filter-reloaded, restart, session-closing, symbol-mapped events and bus admin messages are records on the same ring as the data, written by the same single writer. They are never delivered on a side channel.

**Rationale**: what apps need is ordering relative to data ("the gap came before this trade"). A side channel loses it.

---

## INV-011: Readers map header and data read-only and cannot influence the writer

Readers map a ring's header and data region `PROT_READ`. They may write only their own reader slot, a separate mapping of the slot section after the data region. Readers hold no locks and exert no back-pressure. The writer's hot path never reads slots and never waits for or learns about readers. Only the agent's cold side reads slots, to report them, and it treats their contents as untrusted. All ring atomics are lock-free types, so loads from a read-only mapping are plain loads.

**Rationale**: a hung or buggy app must be physically unable to stall or corrupt the feed for the others.

---

## INV-012: Payload bytes are bit-identical to the bus

The payload of a DATA or BUS_ADMIN record is exactly the SBE message bytes received from the bus (SBE header included). subplane routes; it does not rewrite, normalize, truncate or reorder payloads. The only exception is the last-value cache, which truncates oversize payloads to the header and flags it (`FLAG_LVC_TRUNCATED`); live ring records are never truncated.

**Rationale**: apps decode with the publisher's own generated codecs. Any alteration breaks that contract and moves schema ownership into the agent.

---

## INV-013: The epoch changes on every writer start and is observable

Each agent start sets a new `epoch` (start time in ns), writes it into every new ring header, reports it in the control `HELLO` and `ATTACH` replies and in `/status`, and emits `AGENT_RESTARTED` as the first record on each fresh ring. Old ring files are marked CLOSED and unlinked. A writer is never resumed on an old ring.

**Rationale**: readers must be able to tell "same writer, quiet" from "new writer, state lost" and re-snapshot accordingly.

---

## INV-014: The only hard requirement on a publisher is its schema

subplane must not require publishers to add headers, framing bytes or a particular sequence field. For SBE, message length comes from walking the schema, so not even a length prefix is required. For DBN the record carries its length. For FlatBuffers the requirement is the field ids of the routing fields (from the `.fbs`, no schema file loaded) plus a length: the size prefix, a framer prefix, or one buffer per datagram. For encodings that are not self-delimiting (planned: protobuf, fixed-offset layouts) the requirement is the schema plus a length prefix, a count, or one message per datagram. The payload delivered to apps is the publisher's complete framing unit, bit-identical, never a slice of one (a FlatBuffers sub-table or an SBE group entry is not a message). Packet layout is described in YAML and checked with `subplane probe`. If a bus cannot be described by the framing block, extend the framing block, not the publisher.

**Rationale**: publisher-side requirements were identified as the main adoption risk. SBE already fully describes message length.

---

## INV-015: The app contract is transport-agnostic

Nothing an app can observe through a ring or the control channel identifies the bus transport. A new transport (Aeron, Kafka, replay) must map its failure modes and liveness onto the existing control vocabulary (gap, degraded, recovered, heartbeat, sequence reset) and its per-partition ordering onto `TickHeader::sequence`. Transport-specific control codes, header fields or flags are not added; if a transport cannot be expressed, the vocabulary is extended once, for every transport, with a `RECORD_VERSION` bump.

**Rationale**: the decoupling of apps from the bus is the durable value of the agent. Bus migration without an app release, mixed buses on one box and a single recovery path in every app all depend on it. One transport-specific leak into the contract re-couples every app.

---

## INV-016: The app contract is a public, versioned specification

The ring layout, the 64-byte record header, the control event payloads and the control channel protocol are a specification, not a C++ implementation detail. Rules:

- Every layout is fixed-width, little-endian, with explicit padding and a `static_assert` on size and offset, so it can be implemented in any language from the headers alone.
- Changes are additive within a version (reserved bytes consumed, new control codes, new ops); any incompatible change bumps `RING_LAYOUT_VERSION`, `RECORD_VERSION` or the control protocol version, and readers refuse mismatches (INV-009).
- `core/message.h`, `shm/ring_layout.h`, `shm/control_events.h` and `control/control_proto.h` depend only on each other, the C++ standard library and libc. No agent-internal header is included from them.
- A record is a fixed header tuple followed by the payload bytes. Everything an app needs to route or interpret a payload that lives outside the payload (packet header fields, prefixes, group-resident ids) is lifted into the header tuple or the declared extension tuple (planned); an app is never handed a parent block.
- A second-language reader (planned: Python) is the acceptance test that the contract is real.
- **The agent carries compatibility, apps never have to.** A new agent works with app binaries built against every earlier SDK of the same contract version. A feature that needs app-side code (reader slots, `IDENTIFY`) is opt-in: an older app simply does not use it, and gains it at its own next rebuild, never because the agent changed. New fields go where older readers ignore them (reserved or zero bytes, after the data region, new codes and ops that old clients never see); a reader detects their presence (a nonzero offset, a capability bit, an `UNSUPPORTED` reply) rather than assuming it.

**Rationale**: the agent standardizes how an application box subscribes to a bus. A standard that lives only inside one SDK is a library, not a standard. Agents are one per box and cheap to roll out; apps are many, owned by other teams, and rebuilt on their own schedule, so the cost of change falls on the agent.

---

*Last updated: 2026-10-16*

---

## INV-017: Data flows one way; control flows upstream only to declared services

The agent never publishes data: not onto the bus, not back to a producer, not to another host. It may send control upstream, and only to services named in its configuration: recap requests to a partition's snapshot service (planned). Upstream control runs on a cold-path thread and never blocks the pipeline (INV-002). Moving data between hosts or sites is the bus's job or the relay's (planned), which is a separate process.

**Rationale**: recovery needs a request path, and making it per box (not per app) is what stops publishers being flooded. Keeping data one-way and naming every upstream peer in config keeps the agent a router, not a broker.
