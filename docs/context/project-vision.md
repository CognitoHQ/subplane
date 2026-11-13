# Project Vision: subplane

Status key: everything below is implemented unless marked "(planned)".

## Identity

subplane is the subscription plane under the apps: a single-binary routing agent, one instance on each application box. It subscribes to a firm's internal SBE bus, decodes only the fields needed for routing, filters per application, and fans the original SBE bytes out to local apps through shared memory.

It is not a decoder: it never fully decodes a message, and apps decode SBE with their own generated codecs. It is not a broker: data flows one way (bus to agent to apps) and apps never publish through it. Control may flow upstream, but only to services declared in config: the planned recap path sends snapshot requests to a publisher's sister snapshot service on behalf of the box (see "Recovery: recap").

It is an agent in the Datadog / Fluent Bit sense: one static binary, built-in components, behaviour selected by YAML. There is no `dlopen` and no `.so` loading.

## The thesis

Applications that consume bulk feeds and subscribe to subsets spend most of their complexity on deserialization, filtering and routing. Every app on the box repeats that work, and every app has to track schema changes. Pushing it into one agent next to the socket removes it from every app.

The extra hop is paid back by the app-side waste it removes: no per-app multicast join, no per-app filter code, no per-app parsing of messages the app discards. Latency matters, but the product is simplification with a latency budget, not a nanosecond feed handler.

## Why the scope is narrow

An agent with a limited, well-defined scope (subscribe, route, fan out) can be optimized to an extreme that a general framework cannot reach. The component/activation model then buys calculated flexibility where it pays: one agent serves organizations with several distribution technologies (multicast, Aeron, Kafka) behind the same app contract, and apps shed deserialization, filtering and routing entirely. Narrow core, flexible edges, simpler apps.

UDP multicast (and plain UDP unicast), TCP byte streams and Aeron (IPC, UDP unicast and multicast) ingest are implemented behind one transport interface; Kafka is planned behind the same interface.

## One app contract for any bus

Apps that read a bus directly are coupled to the transport three times over: they link its client library (a media driver for Aeron, librdkafka for Kafka), they encode its framing (length prefixes, per-packet batching, where the sequence lives), and they inherit its failure model (a multicast gap looks nothing like a Kafka rebalance). Change the bus and every app changes.

subplane collapses all three into one contract: a ring of 64-byte `TickHeader` records followed by the original SBE bytes, plus a fixed control vocabulary (gap, degraded, recovered, restarted, filter reloaded, session closing, symbol mapped, bus heartbeat, sequence reset). Transport, framing and liveness semantics are mapped onto that vocabulary inside the agent, so an app never learns which bus it is on.

What that buys a firm:

- **Bus migration without an app release.** Moving a partition from multicast to Aeron, or shadowing both during a cutover, is a YAML change on the agent. Apps are untouched and can be compared against two rings.
- **Mixed buses on one box.** Equities over multicast, a vendor feed over Aeron, reference data over Kafka, all delivered through the same reader.
- **One recovery path.** Every transport's failure modes arrive as the same control events, so apps write the late-join idiom once.
- **Dependency hygiene.** Apps link two headers and nothing else. No media driver, no client library, no version skew between app teams.

The contract is the product. The reader headers, record layout and control protocol are treated as a public, versioned specification (INV-009, INV-015, INV-016), not as an implementation detail of the C++ SDK. Readers in other languages are expected and welcome (planned: Python).

The one-line version: one app contract for any bus; transport, framing and routing are agent configuration, not application code.

## Why a shared implementation, when every large shop could build this

Every shop with many apps per box already has a home-grown "subscribe to the bus, filter my symbols, hand me the bytes" layer, usually a library linked into each app. That is not an argument against subplane; it is the same situation Aeron faced. Every serious shop had its own reliable multicast transport, and Aeron still won, because once a good open implementation existed the custom one became a liability: nobody wants to maintain it, new hires do not know it, and vendors do not speak it. The argument was never "you cannot build this", it was "you should stop owning this".

The same applies one layer up. Aeron standardized how bytes move between processes; subplane aims to standardize how an application box subscribes to them. What makes that credible rather than aspirational:

- A published contract (two headers, a documented record, a versioned ring layout, a control protocol) that anyone can implement in any language without linking our code.
- Publisher requirements near zero (INV-014). A standard that demands publishers change loses before it starts.
- Transport independence (above). No single shop's custom layer bothers to do this, and it is what lets a firm migrate buses without touching apps.
- An operational surface in one place: gaps, degraded topics and partitions, per-session drops, snapshots and filter reloads, visible over HTTP. Custom layers almost never have this because each app team built only what it needed.
- A licence a firm can run it under without asking: FSL-1.1-ALv2 allows any use except a competing commercial product, internal production use included, and each release becomes Apache-2.0 two years after it ships.

## Consumption models

The same contract serves three kinds of consumer. Each has a runnable example; all of them work against the quick-start setup in `README.md`.

### Resident applications

The classic case: a strategy or pricing process that runs all day. It creates or attaches to a session with its filter, seeds state from the snapshot, then tails the ring (the late-join idiom). Example: `examples/shm_demo_app.cpp`.

```bash
build/examples/shm_demo_app --socket /tmp/subplane.sock --session pricer --symbols 1,2,3
```

The equivalent static session in YAML, for apps that should not have to create their own:

```yaml
sessions:
  - name: pricer
    ring: { capacity_mb: 64, max_payload: 4096 }
    topics: [eq.sample]          # patterns: `*` one segment, a final `>` the rest
    symbols: ["1", "2", "3"]
    msg_types: [TRADE, QUOTE]
```

### Elastic and short-lived workers

The expensive and slow parts of consuming a bus are per box, not per app: the multicast join, socket buffers, the busy-polling core, the schema parse, the symbol table, sequence tracking. The agent pays them once and keeps them warm. A worker that starts costs only a session: one control round trip, a ring file, a filter table. With the snapshot it is live with current state in milliseconds. A worker that stops drops its session; the linger window covers a crash-and-restart without the agent noticing.

This turns scaling a market-data consumer up or down from a networking event into a file open. It covers burst jobs (open and close, risk recalcs, end-of-day), containers on a shared box (agent as a host service or DaemonSet, pods sharing the ring directory and control socket through a host mount, no IGMP churn, no per-pod socket tuning), scale-to-zero (only the agent stays resident), and blue/green comparison (two app versions on identically filtered sessions receive the same records in the same order).

Example: the demo app with a time limit is a burst worker. Run several concurrently under different session names and watch `/sessions`:

```bash
for i in 1 2 3; do
  build/examples/shm_demo_app --socket /tmp/subplane.sock --session worker$i --trades-only --quiet --seconds 10 &
done
curl -s localhost:8080/sessions
```

Measured cost per extra session is about 30 ns per message on the pipeline thread (see the latency budget).

### Function-style workloads

Lambda-style, meaning on-box function workloads, not a cloud FaaS (which has no shared memory, no co-location and no multicast). A session filter is a trigger spec: these symbols, these message types, these topics. The ring is the event queue, already ordered, durable across consumer crashes and position-addressed so a consumer can resume or replay. The snapshot is the context a stateless function needs before its first event. The control channel is the registration API. The agent is the event source; functions never own a subscription, a socket or a schema.

A runtime on top is one long-lived host process per box that attaches a session (or a few, with different filters) and dispatches records to small functions in-process or to a warm pool. Because the ring is immutable, a failed function can be re-invoked on the same record, and a new function can be dry-run against the history the ring still holds before it goes live. Typical uses: alerting and surveillance (hundreds of tiny rules fed by one subscription), per-symbol-family sharding, enrichment side channels (dashboards, ledgers, notifications) that must not touch the strategy process.

Example: `examples/rule_runner.cpp`, a minimal runtime. Rules declare a trigger and a body; the runtime unions the triggers into one session filter, seeds through the snapshot, and dispatches. `--from-start` replays ring history through the rules first.

```bash
build/examples/rule_runner --socket /tmp/subplane.sock --from-start --seconds 10
```

### Limits that apply to the elastic and function cases

- **Ring per session.** Memory grows linearly with instances; fifty identical workers means fifty rings. The fix is a per-partition payload arena with header-only session rings (roadmap item 2), which also keeps fan-out cost per session at one cache line when a bus sends large units such as batched FlatBuffers or book snapshots; it is on the roadmap because these two cases depend on it.
- **One pipeline core per thread group.** Per-session cost is small, but many sessions on a hot partition still serialize on one core. Thread groups are static at startup, so capacity is provisioned ahead of a burst.
- **Hard caps.** 256 sessions per partition; fine for apps, tight for a fleet of tiny workers.
- **Snapshot is last value, not history.** Anything beyond the ring's capacity needs a separate archive. State that is not a last value (books, accumulated volume) needs recap (planned).
- **Parallel functions break ordering.** The ring is ordered per session; a runtime that runs functions concurrently owns per-symbol ordering. Sharding by symbol into separate sessions keeps the guarantee.
- **Invocation granularity.** A process per record is hopeless at tick rates. Functions run in-process or in a warm pool; the runtime, not the agent, chooses the isolation level.
- **Session adoption** by a replacement instance with a new identity (an orchestrated pod) is not designed yet; today the replacement attaches by name within the linger window or creates a new session.

## Topology

```
Exchange -> firm feed handler -> internal bus (topics x partitions, published SBE schema)
                                        |
                              subplane agent (one per app box)
                  subscribe -> frame -> routing decode -> filter -> route
                                        |
                          shared-memory ring per session (/dev/shm)
                                        |
                  local apps (decode SBE themselves with generated codecs)
```

subplane does not talk to exchanges. Its input is a firm's own bus, which already carries SBE messages described by a published schema.

The bus is organized as topics, each holding partitions; a partition is one stream with one publisher and one sequence. Apps subscribe to topics and never see partitions, which are an operator's view (architecture.md, "Topics and partitions"). Per-instrument sequence is implemented (2026-11-11): a publisher counts each instrument across the topic, the agent drops duplicates, detects instrument gaps and restarts, and the record's `sequence` becomes the instrument's. Planned: a resolver served by the snapshot service, make-before-break moves of a ticker between partitions, and a gateway publishing one topic over several partitions.

## What apps get

- A 64-byte routing header (`TickHeader`) in front of the original SBE bytes: symbol id, message type, venue, topic, bus sequence, exchange timestamp, agent ingest timestamp, template and schema ids.
- In-band control events on the same ring, ordered with the data: sequence gaps, topic degraded or recovered, filter reloaded, agent restarted, session closing, new symbol mapped, plus bus admin messages (heartbeat, sequence reset).
- A last-value snapshot over the control channel, so a late joiner can seed its state and then tail the ring without a hole (the late-join idiom). The last-value cache only holds self-contained values (see "Recovery: recap").
- Recap: full state for a symbol (a book, accumulated statistics) from the publisher's snapshot service, merged into the ring by sequence, with in-band pending, complete and failed events.
- Dynamic sessions: an app creates, reconfigures and destroys its own subscription over a Unix control socket. Operators can also define sessions in YAML and reload them.
- A header-only client SDK (`shm_reader.h`, `control_client.h`) that depends only on libc and the C++ standard library.

## What apps do themselves

Apps decode the SBE payload with their own codecs generated from the publisher's schema. subplane delivers the bytes bit-identical to the bus. It does not normalize, convert, or reinterpret payloads.

## Latency budget

Tier: tens of microseconds on kernel sockets (`recvmmsg`, optional busy-poll). A "low microseconds" claim was withdrawn: kernel UDP alone is 5 to 20 microseconds, so single-digit microseconds was never reachable without kernel bypass.

First measured numbers (2026-10-12). These are loopback and in-process figures on a 12-core dev box, not a production NIC:

- In-process bench (`subplane replay --bench`, pinned pipeline thread, no busy-poll, 3 SBE messages per packet, 100 symbols, batch 32, 16 MB rings). Frame, routing decode and filter cost about 100 ns per message. Each attached session adds about 30 ns per message (filter check plus ring memcpy).

  | Sessions | p50 ns | p99 ns | p99.9 ns | msgs/s |
  |---|---|---|---|---|
  | 0 (poll only) | 5119 | 10239 | 16383 | 9.77M |
  | 1 | 7167 | 14335 | 24575 | 7.56M |
  | 4 | 12287 | 28671 | 49151 | 4.58M |

  These are per-batch (about 96 messages) `recv_to_publish` values, taken as histogram bucket upper edges, so they can be up to 25% coarse.
- Live run on loopback UDP: p50 895 ns and p99 2.6 microseconds from kernel receive timestamp to ring publish, per packet.

Caveat: the kernel socket, not the agent, is the floor. A busy-poll run on a real NIC has not been done yet.

## Recovery: recap (partly implemented)

The usual way a late-joining, restarted or gapped app recovers is to ask the publisher for state. With many apps that overloads the publisher, and it is the failure this section designs out.

**Two kinds of state.**
- **Self-contained:** the newest message for a key is the whole state for that key; nothing before it matters. Instrument definition, trading status, last quote, last trade. The agent's last-value cache serves these over the control channel. (It keeps the newest record of every data type, but for the types below that is only the last change; a partition carrying them can set `lvc: off`.) That works with no infrastructure beyond the agent.
- **Built up from many messages:** a book from book updates, session volume and VWAP from trades. Rebuilding it needs the publisher's rules (session boundaries, busts and corrections, which trades count, what a gap does to a total). The agent never holds or computes this state.

**The snapshot service.** A publisher that wants late joiners to recover built-up state runs a sister snapshot service. The service subscribes to the same stream the apps get, keeps the state using the publisher's own rules, and answers recap requests with "state as of sequence S" in the subplane protocol (SPP, `docs/protocol/subplane-protocol.md`): a `RecapBegin` stating S, one `RecapRecord` per stream message (the stream's own bytes, carried opaque), and a `RecapEnd`. A firm bus whose service already answers in the bus's own schema between begin and end markers can keep doing so (`reply: stream_markers`). The publisher only publishes and never sees a recap request. The service is written for its schema, by the people who know it; it is optional, and a publisher without one keeps the last-value path. It is named after the service it serves, with a `-snap` suffix: the feeds gateway `spwsgw` gets `spwsgw-snap`, and a firm's publisher `mdpub` would get `mdpub-snap`. There is no generic snapshot service: one is needed for each schema, and one runs beside each publisher that needs one: one per partition, since each partition has its own publisher.

**Many agents, one service.** The load that matters is not one agent asking for many symbols but every agent asking at once: a gateway restart, a blip on a shared segment, a fleet deploy. A service may therefore answer on a **section stream** shared by all the agents of its stream (implemented, subplane-feeds' `spwsgw-snap`): a request is interest, and the service publishes each wanted instrument once, as a section, however many agents asked within its gather window, paced to a byte rate. A partition-wide resync is one **bulk** request per agent, gathered over a longer window, so a gateway restart costs one section per instrument for the whole fleet. A service rebuilding its state keeps the interest and publishes when the instrument is whole instead of refusing, so a restart does not turn into a retry storm. A host-wide rate budget shared by several services (per interface, per topic) is planned as its own component.

**What the agent does.** One agent per box asks on behalf of every app on the box. It only decides whether a recap and a merge are needed, then does the mechanics; it never interprets state.
- **Whether:** a snapshot service is declared for the partition, the session asked for recap, and something triggered it (the session started or added symbols, a sequence gap, the transport went down and came back, the publisher restarted).
- **Coalesce:** one request per symbol per box, however many sessions are waiting; one bulk request per partition for a partition-wide resync; on a topic with several partitions, the symbol is asked of the partition it lives on.
- **Hold:** from the request on, live messages for that symbol are held for the waiting sessions only; sessions already live are never delayed.
- **Merge:** deliver the snapshot, drop held messages with sequence at or below S, deliver the rest in order, mark the symbol complete at S. If the held messages do not reach back to S+1, the snapshot is too old and the agent asks again.

**Schema-neutral.** The request and the reply envelope are SPP, subplane's own small control protocol; the records inside are the stream's own messages, which the agent never parses until it merges them, so any stream encoding can recap (SBE, FlatBuffers, DBN). In `stream_markers` mode the agent instead learns the begin and end markers, S and the request id from decoder configuration, the same way it learns every routing field. The canonical schema is the first user, served by a snapshot service in the feeds package, not a requirement.

**Aeron.** Aeron gives the transport pieces (request and response channels, exact stream positions, and message replay through the Java Archive). It does not give per-key state as of a sequence for many edge subscribers, coalescing on the box, or an app-facing recovery contract. Recap is that layer, and it also works on buses that are not Aeron.

**Across networks.** Cascading between sites is a separate relay process (planned), not the agent. The relay tunnels the bus (gateway) protocol, never an exchange protocol: Aeron fragments in, the same bytes out over TCP, and back into Aeron at the far site, with sequence numbers untouched so merges stay valid at every hop. A relay can also answer recaps for the sites below it, coalescing their requests so each level of the tree asks upstream once. Coordinating recap across hosts beyond that (which agent asks, shared budgets between sites) is parked.

## Minimal publisher requirements

The only hard requirement on a bus publisher is its SBE schema.

- Message length is computed by walking the schema (root block, groups, varData), so no length prefix is required. Back-to-back SBE messages in a datagram parse with no framing at all.
- Packet framing (packet header fields, per-message prefixes, where the sequence number lives) is described declaratively in YAML, not coded.
- `subplane probe` validates a candidate framing block against live packets, so an operator can write a framing block for a bus they do not control.

If the bus carries no sequence number, gap detection is off for that partition and liveness relies on the heartbeat timeout alone.

Recap adds one optional requirement, only for publishers that want it: a snapshot service that answers recap requests in SPP, carrying the stream's own messages as records, and states the sequence S its state is as of, in the same sequence space as the live stream.

**Checked against a real bus (2026-10-12).** Public CME MDP 3.0 packets (27 packets, 31 messages, wire schema versions 6 and 9, from three public repositories and a Databento sample) were replayed through `subplane probe` with a framing block written from CME's documentation alone. With CME's v9 schema every template is known and every message length from the schema walk matches the declared length; with the v13 schema the two retired templates are reported unknown and nothing else changes. Two walker bugs were found and fixed on the way (program-relative jumps for multi-entry groups, and `sinceVersion` skipping). One limitation was found: CME's incremental messages carry the instrument id inside a repeating group, one message per several instruments, so that bus routes by message type until per-entry symbol routing exists (roadmap). `scripts/fetch-cme-sample.sh` plus `tests/integration/test_cme_probe.sh` reproduce the check.

**Second encoding, second transport (2026-10-12).** A real Databento DBN file (CLX5 market-by-order, GLBX.MDP3, 38,212 records of 56 bytes) was served over TCP by `subplane dbn-sim` into a partition configured `transport: tcp_stream`, `decoder: dbn`, `framing: {length: packet}`: every record decoded, none unknown, all 38,212 reached a session filtered to that instrument, recv-to-publish p50 127 ns, p99 383 ns, p99.9 639 ns per record on loopback, unpinned. The same ring contract served both the SBE and the DBN partitions, which is the "encoding is per-partition configuration" rule proved with two formats. Found on the way: DBN's `sequence` is the venue's packet sequence shared by sibling records (so it must not be used for duplicate detection), passthrough instrument ids need `max_symbols` above the largest id and make the dense last-value cache large, and static sessions were attached after the first packets of a fast stream (fixed: commands are applied before polling). `ctest -R dbn_sim` reproduces the synthetic version.

**Third encoding, no library (2026-10-12).** FlatBuffers, routed with a hand-written reader against the published wire format (root offset, vtable, field ids) and no FlatBuffers code in the agent. A fixture of 300 messages built by `flatc` from a bus-like schema (union body, instrument two tables deep, a field old producers never set, file identifier) is routed identically to the generated accessors in the unit tests and replayed over UDP, 1 to 4 buffers per datagram, through the agent to a filtered ring with every payload bit-identical, prefix included (`ctest -R flatbuffers`). Cost on the dev host: about 57 ns per message for dispatch plus four routing fields, bounds-checked. This is step 1 of the encoding-agnostic sequence in the roadmap: apps on a FlatBuffers bus get raw delivery today with their own codecs.

## Non-goals

- The agent is not an exchange feed handler: no ITCH, FIX, WebSocket or other exchange protocol decoding in the core. Feed handlers ship beside it as separate gateway processes in the `subplane-feeds` package (planned), publishing canonical SBE into `shm_ingest`.
- Not a broker. No request-reply for apps or producers, no routing back to a producer, no cross-box delivery. (A local app publishing one-way into an ingest ring is planned; see roadmap. Recap requests to a declared snapshot service are control flowing upstream, not data, and cross-site cascading is a separate relay process.)
- Not a full SBE decoder. Routing fields only; apps own full decode.
- No dynamic loading of foreign code (no `.so` plugins, no WASM, no embedded scripting).
- No cross-host fan-out from the agent. Fan-out is local to the box; the relay (planned) is a separate process.
- No state building in the agent. Books, accumulated statistics and anything else that needs the publisher's rules belong to the publisher's snapshot service.
- No payload transformation or normalization in the raw mode, which is the default on every partition. Normalization to the canonical flat schema is an opt-in per-partition mode (planned, encoding-agnostic step 4), never applied to raw sessions.

## What was rejected and why

- **WASM / VM decoders and code generation.** No speed gain; filtering needs only tables. The real value of WASM (sandboxed app-supplied logic) is a later feature. The decoder spec is kept clean so a backend could be added.
- **Rust rewrite.** No stable ABI, same C-ABI endpoint, ecosystem and familiarity favour C++20. Switching cost is lowest now, and the decision is noted as revisitable.
- **ITCH as the first target.** ITCH is order-by-order, so nothing app-useful comes out without a book builder. The thesis is bulk tick feeds that already carry trades, quotes and book deltas.
- **Aeron IPC as the v1 app contract.** It would make the Aeron client API part of the app contract. The v1 contract is a small header-only reader under our control. Aeron ingest is implemented (2026-10-13; the media driver is now a build dependency), so an `aeron_ipc` fan-out component beside the shared-memory ring is back on the roadmap (item 9), as an option, not the contract.
- **A general app publishing API** (rejected 2026-10-12, narrowed the same day). A free-form app-to-agent-to-apps path would make subplane a broker. What is planned instead is `shm_ingest`: a local app writes one-way into a ring the agent reads as just another partition (schema, framing, sequence, routing, fan-out), so derived data produced on the box reaches every other app over the same contract. Flow stays one-way per partition; nothing is routed back to its producer; leaving the box is still the bus's job. Reason for the change: every shop generates derived data and needs a publishing layer, vendors do not offer one, and small shops have no bus for it.
- **`dlopen` plugins.** They contradict zero-cost dispatch and cross a C++ ABI. YAML activation of compiled-in components gives the operational flexibility without foreign code.

## Roadmap

Items are planned unless marked done. The order is the intended build order; each item is justified by a use case above or a finding in the decision log. Items 16 and 17 (recap and the relay) are being built next, ahead of item 1.

1. **Routing from parent blocks, per-entry symbol routing, and the extension tuple.** One record-version bump covering three related changes. (a) Any routing field (`symbol`, `sequence`, `exchange_ts`, `venue`) may be sourced from `packet_header.<f>`, `message_prefix.<f>` or `message.<Field>`, so discriminators that live in a parent block (CME's sending time and channel) reach the header tuple without the app seeing the parent bytes. (b) A per-partition **extension tuple**: a YAML-declared list of named fixed-width fields sourced the same way, written between the 64-byte header and the payload (`ext_len` in the header's reserved bytes), with the layout published via `HELLO` and `/partitions`; readers that do not care skip it by length, the payload stays bit-identical. (c) **Per-entry symbol routing** for instrument ids inside a repeating group (CME MDP 3.0 incremental refresh): decode the group's symbol field per entry, route the message to every session matching any entry, and write the matched ids into the extension tuple so an app knows which of its instruments the message touched without walking the group.
2. **Payload arena with header-only session rings** (planned): each framing unit written once to a generation-stamped shared-memory arena per partition, session rings carrying the 64-byte header with a reference to it, so fan-out per session costs one cache line whatever the unit size and elastic workloads scale in sessions without scaling in memory. Inline payloads stay the default for tick-sized units. Never ref-counted: an overwritten slot is an overrun, and no reader can stall the writer.
3. **Generic schema-driven SBE decoder** on the app side: root fields to columns, groups to nested rows, driven by the same XML the agent loads, no generated code. Serves the kdb+ feed, the Python client and the recorder index alike.
4. **Recorder and replay** (in progress; first-release scope): record what a partition **received**, not what the agent made of it, as standard pcapng (`docs/context/recording-format.md`) with the partition's configuration and schema embedded in every segment, so a recording does not depend on the ring or record format and reads without the config that was live that day. The agent copies each received unit into a per-partition tap ring (built); `subplane record` writes rotating segments with explicit loss blocks (planned); a `pcap` transport replays a recording, or any pcap or pcapng capture, through the agent so every app runs unmodified against history (planned). Built: the pcapng reader and writer, pcapng input to `subplane replay --pcap`, and the tap ring. Later: recording one app's output for forensics, native DBN files, export to rows (needs item 3).
5. **`shm_ingest` transport**: a local app publishes one-way into a ring the agent reads as a partition (see "What was rejected"). Gives a Databento-fed or bus-less shop a complete on-box data plane and lets derived data reach every app over the standard contract.
6. **kdb+ feed** (`sp_kdb`): a bundled consumer that decodes with the generic decoder, batches, and publishes to a tickerplant through the kdb+ C API, with template-to-table mapping in YAML; with `shm_ingest`, q processes publish derived data back to the box. Target: a KX-listed interface.
7. **`databento_live`**: the DBN decoder and the `tcp_stream` transport are implemented (2026-10-12, see "Checked against a real bus"); the live gateway adapter (CRAM authentication, subscription messages, symbology) on top of `tcp_stream` is left for a later contribution, possibly from outside the core team, and is not scheduled.
8. **Python client** (a thin binding over `shm_reader.h`), also the proof that the app contract is implementable without the C++ SDK, and the host for function-style runtimes.
9. **Aeron**: ingest done (2026-10-13; the `aeron` transport, Aeron C client and media driver fetched at build time, publishers in the feeds package publish Aeron streams of canonical SBE); an `aeron_ipc` fan-out component (untethered subscriptions) remains planned.
10. **Kafka transport** (an earlier producer is parked under `src/transport/parked/`).
11. **Standalone specification** of the ring layout, record header and control protocol, independent of the C++ headers.
12. **Derived feeds** (VWAP, NBBO, spread, book snapshots) computed once by the agent and published on the same rings; with `shm_ingest` in place these become the agent's own ingest partitions.
13. Last-value cache exported into shared memory for direct app reads.
14. Real bus capture on a production NIC with busy-poll, to replace the loopback numbers; kernel-bypass evaluation after that.
15. **Feeds package** (`subplane-feeds`, a sister repository, crypto first): started 2026-10-13. One `spwsgw` binary (subplane WebSocket gateway) driven by per-exchange YAML packs (connection, subscribe templates, frame routing, JSON path to canonical field mapping with a closed vocabulary and a shared registry of conversions) plus compiled-in strategies for what YAML cannot say (order-book sync protocols); it publishes canonical SBE over Aeron, one topic per exchange, which the agent ingests as an ordinary `aeron` partition of that topic, so a box holds one exchange connection and every app reads a filtered ring. Binance spot, Coinbase, Kraken, Bitstamp and OKX are live (trades, quotes, books where the protocol is covered), each with a recorded fixture replayed byte for byte and end to end through `subplaned`. The same shape makes the agent the only Kafka consumer on a box once 10 exists. The sister repo depends only on the agent's public contract (the canonical schema, and one or more canonical messages back to back per Aeron offer, schema-framed) and never the reverse.
16. **Recap** (see "Recovery: recap"). Groundwork done (2026-10-14): instrument definitions have their own message type and cache slot, and integer instrument ids can be scoped by venue. Done since (2026-10-15): in-band transport up/down events, reply markers in decoder config, per-partition `lvc: on|off`, the `recap:` block with the `aeron` recap component and the recap manager (requests, coalescing, retries, reply assembly), and the hold and merge on the pipeline thread for sessions that ask for recap (session start and filter changes as triggers); recap on SPP (`Hello`, `RecapRequest`, SPP replies by default, DBN recap); the failure triggers (gap, sequence reset, publisher restart) with `SYMBOL_STALE` and renewed requests (2026-10-16). Recap per partition with shared sections (2026-11-12): sections matched by instrument on a section stream every agent reads, bulk resyncs, the snap's interest, gather windows and pacing, and on topics with several partitions recap asked of the partition a symbol lives on. Then: the SDK's recap helper with `views_app --recap`, and the canonical snapshot service `spwsgw-snap` in the feeds package, with the gateways emitting a book Clear on resync so any replica stays exact.
17. **Relay** (planned): a separate process that tunnels the bus between sites, Aeron in and TCP out or TCP in and Aeron out, bytes and sequence untouched, heartbeats on idle links, a bounded buffer per connection; optionally the recap service for the sites below it.
18. **subplane-lib** (started 2026-11-13): rate limits and primary/secondary election shared by processes, as a library over Valkey with no daemon (architecture.md, "subplane-lib"). Built and tested on its own; then wired in: recap section pacing in the snap per interface and topic, exchange API limits for gateways, agent request rates; gateway and snap pairs per partition with the epoch carried in what they publish and checked by agents. Later: borrowing between sibling buckets, an etcd store for sites that need consensus.

**Encoding-agnostic apps**, a sequence tracked alongside the numbered items; every step ships as a mode that stays:

1. Raw `flatbuffers` decoder: done (2026-10-12). Field ids in YAML, whole buffer delivered bit-identical, no library.
2. Typed views for flat SBE schemas: done (2026-10-12). `subplane schema --views` emits a C++ struct or memcpy view (decided by alignment, never packed), a NumPy dtype and a q `1:` spec from any group-less XML; the sample schema's views are generated and compiled at every build.
3. The canonical flat SBE schema: done (2026-10-12). `schemas/subplane_canonical.xml`: trade, quote, book update, ten-level book snapshot, OHLCV, status, instrument definition and heartbeat as fixed, naturally aligned blocks with a common 40-byte prefix (since 2026-11-11, with InstrumentSeq) and int64 prices scaled by 10^-9; every message is a struct view, generated and compiled at every build, with `examples/views_app.cpp` consuming it end to end. No transcode yet.
4. Transcode to the canonical schema (planned): a per-partition mapping in the decoder YAML from source fields to canonical fields, with scale and unit conversion, validated at startup; apps written against the canonical struct run on any bus. Raw delivery stays the default.
5. App-attached target schema (planned): a session names its own flat SBE XML, the daemon resolves the mapping by field name with aliases, one transcode per distinct schema.
6. `subplane schema` (planned): projects tick-shaped protobuf or FlatBuffers messages to flat SBE XML and an identity mapping; a convenience over 4 and 5. A raw protobuf decoder follows the shape of step 1 when a user needs one.
