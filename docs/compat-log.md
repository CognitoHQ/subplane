# Compatibility log

Publishers and apps are tied to the platform version they were built against, so every change that could affect an existing **publisher**, **app** or **operator config** is recorded here. Version numbers (the product version and the contract constants `RING_LAYOUT_VERSION`, `RECORD_VERSION`, `CONTROL_PROTO_VERSION` and the canonical schema version) are frozen until the first release; at that release this log becomes the baseline for the release notes, the compatibility rules and the versions.

**Enforced in CI.** A pull request that changes a contract path (`.github/compat-paths.txt`) must add an entry here, or say `Compat: none (<reason>)` in the PR body or a commit message (`scripts/check-compat.sh`, workflow `compat`). The same script runs locally: `scripts/check-compat.sh origin/main HEAD`. The repository is private on a free plan, so GitHub cannot make the check required; pull requests are merged with `scripts/merge-pr.sh <n>`, which refuses unless `compat-log` and the build have passed. Changes in `subplane-feeds` that alter what gateways emit are recorded here too, because apps see them.

**Impact:**
- `additive`: an old publisher or app keeps working unchanged.
- `behaviour`: no format change, but an old app or config sees something different.
- `breaking`: an old publisher, app or config stops working, or works wrongly, without a change.

**Audience:** `publisher` (what a bus publisher must send), `app` (ring, header, control channel, SDK), `config` (YAML, CLI, HTTP), `feeds-output` (what our gateways emit).

## Baseline (no compatibility promised before this)
Everything up to and including the canonical schema v1 (b8ab03a, 2026-10-12) and the rename to subplane (2be456c, 2026-10-13) is pre-baseline:
- Ring files, the socket path and binary names changed with the rename (`flashtick` → `subplane`, `subplaned`, `subplane` CLI).
- Nothing was released, so nothing is owed for these changes.

## Entries (newest first)

| Date | Repo / PR | Change | Audience | Impact | Migration |
|---|---|---|---|---|---|
| 2026-10-15 | subplane (3.3) | Partition `recap:` block (`type: aeron`, request/response channels, `timeout_ms`, `max_inflight`, `retry`, `reply_slabs`, `reply_slab_bytes`); the 64-byte recap request `RecapRequestWire` (`include/subplane/recap/recap_request.h`, version 1) that snapshot services receive; the reply contract (contiguous BEGIN, records, END on an Aeron response channel); recap counters in `/metrics` (`recap`) and `/partitions` (`recap`) | config, publisher (snapshot services), operator | additive: no existing config or app changes. The request layout is now a contract: a `<service>-snap` written against version 1 must keep working. | none |
| 2026-10-15 | subplane (3.2) | Config: decoder `admin:` names `RECAP_BEGIN`, `RECAP_END`, `RECAP_REJECT` with a `fields:` block (`sbe_routing`, `flatbuffers`; `dbn` rejects them); partition `lvc: on\|off` (default on). New partition counter `recap_markers` (in `/metrics`). | config, operator | additive: existing configs load unchanged. A stream that already carried messages mapped as recap markers would now see them dropped from the ring, but nothing could map those names before. | none |
| 2026-10-15 | subplane (3.1) | Control codes 0xA7 `TRANSPORT_DOWN` and 0xA8 `TRANSPORT_UP` with `TransportStateEvent` (24 bytes); new partition counters `transport_down`, `transport_up` (in `/metrics`); `/partitions` gains `transport`, `transport_down`, `transport_up` | app, operator | additive. Sessions with `agent_control` now receive these events, including a `TRANSPORT_UP` when a source first appears and a `TRANSPORT_DOWN` at start when an Aeron or TCP source is not there yet | Apps that switch on control codes must skip unknown ones (the SDK examples do). JSON readers of `/partitions` see three more fields. |
| 2026-10-14 | subplane #8 | Recap and relay contract documented: planned control codes 0xA7-0xAD, `recap:` block, reply markers, 64-byte upstream request, INV-017 | app, publisher, config | none yet (planned, docs only) | Codes 0xA7-0xAD and 0xAE are reserved; nothing else may use them. |
| 2026-10-14 | subplane #7 | `symbol_scope: venue` (opt-in): integer symbols interned as (venue, id), spelled `<venue>:<id>` in filters, `SYMBOL_MAPPED` names and `/lvc` | config, app | additive (default `global` unchanged) | None unless opted in; then filters use `venue:id`. |
| 2026-10-14 | subplane #7 | `SymbolResolver::resolve_u64(key, venue)` gained a parameter | internal (decoder interface) | not exposed to apps or publishers | n/a |
| 2026-10-14 | subplane #7 | New data `msg_type` 7 `DEFINITION`; the last-value cache tracks types 1-7. Canonical configs map template 10 to `DEFINITION` (was `STATUS`). | app, config | behaviour | An app that took instrument definitions as `STATUS` records, or filtered `msg_types: [STATUS]` to get them, must add `DEFINITION`. Apps that switch on `msg_type` should treat unknown values as skip. Old configs mapping template 10 to `STATUS` still load. |
| 2026-10-14 | subplane #7 | `SNAPSHOT` reply `ring_head_at_snapshot` now read from the published (atomic) head | app | fix (no format change) | none |
| 2026-10-14 | subplane #6 | `views_app --spin/--latency/--dump` | example only | none | n/a |
| 2026-10-13 | subplane-feeds 77c26eb, subplane #5 docs | A gateway Aeron offer carries one or more canonical messages back to back, schema-framed; the agent partition uses `framing: { length: schema, sequence: message.Sequence }` | publisher, config, feeds-output | breaking for an agent configured `length: packet` against a batching gateway | Use `length: schema` for gateway streams. `--batch-bytes 0` restores one message per offer. |
| 2026-10-13 | subplane-feeds 0609849 | Gateway binary renamed `subplane-gateway` → `spwsgw` | operator | breaking for scripts | Rename in scripts. |
| 2026-10-13 | subplane-feeds | Gateway `Sequence` is one counter per process, starting at 1 on each start, consumed even by dropped offers | feeds-output | baseline fact (recap needs `SourceEpoch` to detect restarts) | n/a |
| 2026-10-13 | subplane bc58c3a | `aeron` transport added | config | additive | n/a |

## Known upcoming entries (from the active plans)
- **Recap:**
  - events 0xA9-0xAD;
  - the `recap` session flag in the `CtlSessionReq` pad byte (additive: old clients send 0).
- **Canonical schema v2:**
  - `Heartbeat` gains `SourceEpoch` (additive under SBE rules: block length grows, old readers skip it);
  - templates 21-23.

  Old publishers stay valid; old apps decoding Heartbeat by fixed size must use `blockLength`.
- **Gateway** emits `BookUpdate{Action=Clear}` and full depth on resync. feeds-output: behaviour. Apps must handle `Clear`, which is already in the schema but was never emitted.
- **Symbols (next-phase, unscheduled):** `TickHeader` bytes 56..63 become `corr`. App: additive (reserved bytes were zero). Also `SYMBOL_BOUND`, `SUBSCRIBE`/`UNSUBSCRIBE` ops.

## To decide at first release
- The starting version: 0.x pre-release versus 1.0.0, and the reset from the flashtick-era `2.0.0`.
- Whether the contract constants start at 1 with this log as history, or are bumped once to mark the release.
- Capability flags in `HELLO`/`/status`, so clients detect features without version bumps.
- How long deprecations last (for example `SYMBOL_MAPPED` once names exist).
- `CHANGELOG.md` per repo, generated from this log.
