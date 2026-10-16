# AGENTS.md: using subplane from a coding agent

Read the single file for your task and follow it. Each one is self-contained.

| Task | File |
|---|---|
| Write or change an app that reads market data from subplane (C++: `ShmReader`, `ControlClient`) | `docs/guides/app-developer.md` |
| Stand up a known-good deployment (crypto gateways into an agent, or a firm SBE multicast bus) | `docs/guides/deploy-reference.md` |
| Write or change an agent config for a new bus (transport, framing, decoder field mapping) | `docs/guides/custom-integration.md`, section 1 |
| Publish data into subplane, or design an SBE schema for it | `docs/guides/custom-integration.md`, sections 2-3 (and the `skills/subplane-schema-designer` skill) |
| Write a gateway, snapshot service or upstream service (SPP) | `docs/protocol/developer-guide.md` |
| Operate, monitor or debug a running agent or gateway (metrics, alerts, topology, tracing) | `docs/guides/operator.md` |
| Change subplane's own source code | `CLAUDE.md`, then `docs/context/invariants.md` and `docs/context/architecture.md` |

Facts that hold everywhere:
- The agent's binary is `subplaned`, the CLI is `subplane`, and the crypto gateway is `spwsgw` (repo `subplane-feeds`).
- The app SDK is header-only C++20: `include/subplane/client/shm_reader.h` and `control_client.h`.
- Apps never need rebuilding because the agent changed. Skip unknown control codes and message types, and read `payload_len` instead of assuming sizes.
- The full index, by layer, is `docs/README.md`. Everything there is implemented unless marked "(planned)".
