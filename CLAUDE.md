# subplane: Co-Located Market Data Agent

The agent binary is `subplaned` (CMake target `subplaned`, `src/main.cpp`). The operator CLI is `subplane` (`tools/subplane.cpp`, one subcommand per tool: `probe`, `tail`, `schema`, `send`, `replay`, `dbn-sim`, `aeron-pub`, `soak`).

## What This Is
A single-binary routing agent for the firm's internal SBE bus, one per application box. It subscribes to bus partitions over UDP multicast, finds message boundaries by walking the published SBE schema, decodes only routing fields, filters per application session, and fans the original SBE bytes out through per-session shared-memory rings. Apps decode SBE with their own codecs; they no longer deserialize, filter or route. Not a decoder, not a broker.

## Project Phase: v1 IMPLEMENTED
Hot-path rewrite complete (M0-M8): transport, framing, routing decoder, pipeline, shm rings, control channel, last-value cache, HTTP cold API, tools, tests, CI. Aeron ingest done (roadmap item 9a). Docs reset done (M9). Framing and schema walk validated on real CME MDP 3.0 packets (`scripts/fetch-cme-sample.sh`, `ctest -R cme_probe`). Build order for what comes next is the numbered roadmap in `docs/context/project-vision.md` (per-entry symbol routing, shared rings, generic SBE decoder, recorder, `shm_ingest`, kdb+ feed, Databento, Python, Aeron, Kafka). Licence: Apache-2.0. Repository stays private until it is rebranded under the CognitoHQ organization.

## Tech Stack
Only what builds today:
- **Language**: C++20 (GCC 11, Ubuntu 22.04), CMake 3.20+, `-Werror`
- **Dependencies**: yaml-cpp (system); pugixml and doctest vendored in `third_party/`; Aeron C client + media driver fetched at configure time (`cmake/aeron.cmake`, pinned tag, no Java; needs CMake 3.30+; `SUBPLANE_WITH_AERON=OFF` to drop it)
- **Ingest**: UDP multicast / unicast (`recvmmsg`, optional busy-poll and kernel timestamps); TCP byte stream reassembled into records (`tcp_stream`); Aeron streams, IPC or UDP, optional embedded media driver (`aeron`)
- **Decode**: SBE routing decoder (routing fields only) with declarative framing and schema-walk message length; DBN decoder (Databento, fixed 16-byte header); FlatBuffers decoder (field ids via the vtable, no library). Encoding is per-partition config. Canonical flat SBE schema (`schemas/subplane_canonical.xml`) with typed C++/Python/q views generated at build time by `subplane schema`.
- **Fan-out**: shared-memory ring per session in `/dev/shm`, header-only reader SDK
- **Control**: Unix `SOCK_SEQPACKET` control channel (dynamic sessions, snapshots)
- **Cold API**: HTTP/JSON (`/status`, `/partitions`, `/sessions`, `/lvc`, `/metrics`, `/reload`)
- **Metrics**: lock-free log histogram and one-writer counters
- Planned, not built: `aeron_ipc` fan-out, Kafka, derived feeds, Python client

## Vocabulary
**component** = compiled-in code in a static registry; **activation** = YAML entry that selects and parameterizes components (a partition); **session** = runtime app subscription (filter + ring). Never say "plugin" in code or docs without saying which of these you mean.

## Context Documents

**Public docs** (in git):
- **[Project Vision](docs/context/project-vision.md)**: identity, thesis, latency budget, non-goals, roadmap
- **[Architecture](docs/context/architecture.md)**: data flow, ring and record format, control channel, config reference, source layout
- **[Invariants](docs/context/invariants.md)**: non-negotiable engineering rules
- **[Landscape](docs/context/landscape.md)**: agent vs library vs middleware (Fluent Bit, Helix, OpenMAMA, Aeron, etc.)

**Private docs** (gitignored, in `.subplane/`):
- **[Decisions](.subplane/decisions.md)**: Design decision log
- **[Plan](.subplane/plan.md)**: Current/next sprint plan
- **[Tasks](.subplane/tasks.md)**: Backlog and unplanned work
- **[Lessons](.subplane/lessons.md)**: Corrections and patterns to avoid

## Philosophy
- **On-box agent, not middleware.** Runs next to the apps it serves. One bus subscription per box, not per app. One-way flow: bus to agent to apps, no app publishing.
- **Component / activation / session.** Components are compiled in; YAML activates them; sessions are runtime app subscriptions. No `.so` loading, no `dlopen`.
- **Apps decode SBE, subplane routes.** The agent decodes only routing fields and delivers the original bytes bit-identical. The only hard requirement on a publisher is its SBE schema.
- **Narrow core, flexible edges.** Subscribe, route, fan out, and nothing else, so the core can be optimized hard; transports and fan-outs vary behind one app contract.
- **Latency is a budget, simplification is the product.** Tens of microseconds on kernel sockets. Hot path: no heap, no mutex, no I/O, no `std::function`, one virtual call per packet batch. Measure everything.
- **Docs mark implemented vs planned.** Every claim in `docs/context` is implemented or tagged "(planned)".

## Workflow Rules
- **Plan first for non-trivial changes.** Enter plan mode for 3+ step tasks or architectural decisions.
- **Opus 4.6 for planning**: architecture, research, design decisions, reviews.
- **Sonnet 4.6 for execution**: writing code, scaffolding, tests, mechanical changes.
- **Update docs as we go.** Every decision/finding gets captured in the right doc.
- **Lean CLAUDE.md.** Details live in context docs, lazy-loaded when needed.
- **Measure before optimizing.** Use latency metrics to justify performance changes.
- **Private docs stay private.** Strategy and research in `.subplane/` (gitignored).

## Engineering Philosophy
- **No quick hacks.** This is infrastructure: correctness and stability over velocity.
- **`// INTENT:` comments on critical code.** Explain *why* non-obvious logic exists and *what breaks* if removed. Lock-free sections, ring/record ABI, component lifecycle code.
- **Simplicity first.** Make every change as simple as possible. Three similar lines > premature abstraction.
- **Autonomous bug fixing.** Point at logs, errors, failing tests: then resolve them.
- **Capture lessons.** After any correction, update `.subplane/lessons.md` with the pattern.
