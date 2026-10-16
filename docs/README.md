# subplane documentation

Read one file: the one for your task. Each guide is self-contained. It tells you what to do and links deeper only for detail you may want; nothing in it requires reading anything else.

## Start here

| I want to | Read |
|---|---|
| understand what subplane is and why | `../README.md` |
| try it on one machine in 10 minutes | `../GETTING_STARTED.md` |
| deploy a known-good setup (crypto exchanges, a firm SBE bus) | `guides/deploy-reference.md` |
| write an app that consumes data | `guides/app-developer.md` |
| run, monitor and troubleshoot agents and gateways | `guides/operator.md` |
| connect my own bus, publish my own data, design a schema | `guides/custom-integration.md` |
| write a gateway, snapshot service or upstream service | `protocol/developer-guide.md` |

## Layers

Most people stop at the first two.

1. **What and why:** `../README.md` and `../GETTING_STARTED.md`.
2. **Task guides, one per reader:** `guides/` and `protocol/developer-guide.md`. Each covers either using a reference deployment or building something custom.
3. **Reference and specifications:** what the guides summarize, in full.
   - `context/architecture.md`: data flow, ring and record format, control channel, every configuration key, source layout.
   - `context/observability.md`: the metrics, topology and health contract every service serves.
   - `protocol/subplane-protocol.md`: the subplane protocol (SPP), normative.
   - `context/invariants.md`: the engineering rules that do not bend.
   - `compat-log.md`: every contract change and what it means for apps, publishers and operators.
4. **Project:** `context/project-vision.md` (thesis, non-goals, roadmap) and `context/landscape.md` (how subplane compares with other tools), plus `../CONTRIBUTING.md` for working on subplane itself.

Everything is implemented unless marked "(planned)".

## For coding agents

`../AGENTS.md` maps tasks to the one file to read. `../CLAUDE.md` is for agents working on subplane's own source, not for using it.
