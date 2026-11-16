# The subplane protocol (SPP)

Version 1. Status: specified. Implemented: the codec (`include/subplane/proto/subplane_proto_codec.h`), the agent's side of recap over Aeron (`Hello`, `RecapRequest`, the reply messages and sections, `Status`; see `docs/context/architecture.md`, "Recap"), and subplane-feeds' `spwsgw-snap`, which answers with sections. Planned: the gateway's `Hello`, `Republish` and `SourceState` (feeds step 4), the TCP binding, and the app extension relay.

SPP is how subplane processes talk to each other about control:

| From | To | What |
|---|---|---|
| agent | snapshot service (`<service>-snap`) | recap requests; replies, or sections on the service's section stream |
| snapshot service | gateway | republish requests |
| gateway | its subscribers (snapshot services) | source state, on the gateway's state stream |
| app, through the agent | a declared service | extensions (planned) |

Live market data is never SPP. It stays on the bus, in the bus's own schema. SPP does not assume any transport: section 4 says how it rides on each one.

"Must", "must not" and "should" are normative.

## 1. Encoding
- **Schema:** `schemas/subplane_proto.xml`, package `subplane.proto`, `schemaId` 101, SBE 1.0, little-endian, with the standard 8-byte message header (blockLength, templateId, schemaId, version, each a u16).
- **Correlation:** every message's root block starts with `CorrelationId` (u64).
  - A request sets it to a value unique among the sender's outstanding requests. An agent uses its request id; an incrementing counter is enough.
  - Every reply echoes it.
  - Messages that are not requests or replies set it to 0.
- **Layout rules:**
  - root blocks are naturally aligned;
  - group entries are fixed blocks;
  - variable-length data (`varData32`: a u32 length, then the bytes) comes after the groups.
- **Codecs:** `subplane schema --codec cpp` generates encoders and decoders from the schema. The C++ codec for SPP is checked in at `include/subplane/proto/subplane_proto_codec.h` (namespace `sp_proto`), and a test fails if it is stale.

## 2. Messages

| Id | Message | Direction | Meaning |
|---|---|---|---|
| 1 | `Hello` | both ways | Who I am (`Role`, `ServiceName`, `PeerEpoch`), the versions I speak (`MinProtoVersion` to `ProtoVersion`), and my extensions (group `Extensions`: id, version) |
| 2 | `Status` | reply | The outcome of a request with no richer reply. `Code`: Ok, Unsupported, NotReady, BadRequest, Busy, NotFound, Timeout, Denied. Optional `Text` is for logs only. |
| 10 | `RecapRequest` | agent to snap | The state of `Instrument` (and `Venue`) as of a sequence, or with `Flags` bit 0 (Bulk) of every instrument of the stream. Carries `AgentEpoch`, `LastSeqSeen` and `SourceEpochSeen`. |
| 11 | `RecapBegin` | snap to agent | Opens a reply or a section: the records that follow are the state as of `AsOf` (S), built from publisher `SourceEpoch`. A section names its `Instrument` and `Venue`. |
| 12 | `RecapRecord` | snap to agent | One record: `Payload` is a stream message in the stream's own encoding, bytes as the publisher sent them |
| 13 | `RecapEnd` | snap to agent | Closes a reply, with the number of records sent |
| 14 | `RecapReject` | snap to agent | Replaces a reply. `Reason`: Other, NotReady, UnknownInstrument, Busy. |
| 20 | `Republish` | snap (or operator) to gateway | Publish `Instrument` (0 means every instrument) again, on the live stream. Answered by `Status`. The data itself appears in band. |
| 30 | `SourceState` | publisher to its subscribers, one-to-many | Whether the instrument's in-band state can be trusted: `State` Unknown, Stale or Ok, with `Reason` (Startup, Gap, Disconnect, Resync, Republish), as of data-stream `Sequence`, from publisher `SourceEpoch`. `Instrument` 0 is the whole source (the beat). |
| 40 | `Resolve` | agent to resolver | Which instrument a `Name` (the venue symbol) or `Instrument` (and `Venue`) is, and its home partition. `Instrument` 0 and an empty `Name`: the whole topic. |
| 41 | `ResolveReply` | resolver to agent | The topic (`TopicId`, `Partitions`) and a group `Instruments` of `{Instrument, Venue, Partition, Symbol}`, `Partition` being the instrument's home. `Flags` bit 0 Last. |
| 900 | `Extension` | either way | An envelope for an extension: `ExtensionId`, `ExtType`, `Flags` (bit 0 must-understand, bit 1 reply expected), and an opaque `Body` |

**A recap reply** is `RecapBegin`, then zero or more `RecapRecord`, then `RecapEnd`, all with the request's `CorrelationId`. It is sent contiguously: replies to different requests are not interleaved. A service that cannot answer sends a single `RecapReject` instead. S must be in the same sequence space as the live stream. Records carry the stream's bytes untouched, so recap works for any stream encoding (SBE, FlatBuffers, DBN), and apps receive the publisher's bytes unchanged.

**Sections.** A service may answer instead on a **section stream** of its own, one-to-many, which every agent of the stream subscribes to. A request is then interest: the service publishes each wanted instrument once, as a section, however many agents asked for it, and agents take any section for an instrument they wait on, whoever asked.
- **A section** is a reply whose `RecapBegin` names its `Instrument` and `Venue`; its `CorrelationId`, on every message of it, is a section id unique per service epoch. A receiver matches it by instrument, never by request. Sections are sent one at a time, so they never interleave; one may span offers.
- **Answers on the response channel** shrink to what a section cannot say: `RecapReject` (an instrument the service does not have: `UnknownInstrument`) and `Status`. A service may acknowledge a request with `Status{Ok}`; nothing requires it.
- **Bulk.** `RecapRequest.Flags` bit 0 asks for every instrument of the stream, for a resync after something every subscriber saw at once (a gap, a publisher restart). It is answered by sections only, with no end marker: a receiver ends its wait when the stream goes quiet.
- **Coalescing and pacing** are the service's: it gathers interest for a window before an instrument's section is due (a longer one for bulk, which arrives from every agent at about the same time), publishes single requests ahead of bulk, and keeps to a byte rate. A known instrument that is not yet whole keeps its interest and is published once it is; the service does not refuse it as `NotReady`.

**Resolve** is how an agent learns a topic's instruments from the service that publishes them (in subplane-feeds, a snapshot service reading the gateway's instruments file), instead of from the data.
- **One question, three forms:** by `Name` (the venue symbol, exactly as spelled), by `Instrument` and `Venue`, or for the whole topic (`Instrument` 0, empty `Name`).
- **The answer** is one or more `ResolveReply` with the request's `CorrelationId`; the last sets `Flags` bit 0 (Last). A whole topic is paged so that each reply fits one transport message (on Aeron, term length / 8). An unknown name or id is answered with `RecapReject{UnknownInstrument}`; a service without a list answers `Status{Unsupported}`.
- **Every reply carries the topic** (`TopicId`, `Partitions`), so the receiver can check its own configuration against the publisher's. Stream ids are not sent: partition n's is `TopicId << 16 | n`. Transport addresses are not sent: they are the receiver's configuration.
- **The list is a snapshot.** A receiver asks again from time to time to see instruments listed since; a reply replaces what it had.

**Source state** is how a publisher that recovers from its own upstream (a gateway resyncing an exchange book) says so without putting control into its market data. Its subscribers are snapshot services; the agent does not consume it today.
- **Separate stream.** A publisher sends `SourceState` on a state stream of its own, never on its data stream: a data stream carries one schema, the publisher's.
- **Ordered by sequence.** `Sequence` is the last data-stream sequence the publisher sent before the state took effect. A subscriber applies the state once it has applied the data stream up to `Sequence`, so the two streams need no common ordering.
- **What the states mean, per instrument:**
  - `Stale`: the publisher lost its upstream for this instrument (a gap, a disconnect). Its in-band state after `Sequence` is not trustworthy until the next `Ok`.
  - `Ok`: the publisher has republished the instrument's whole state in band, ending at `Sequence`; from there the in-band state is complete.
  - `Unknown`: the publisher has not yet published the instrument's state (start-up).
- **When it is sent:** on every change; for every instrument after `Republish` (with the `Republish`'s `CorrelationId` on its `Ok`); and at least once a second as a beat with `Instrument` 0, whose `State` is Ok while the publisher is connected upstream and Stale otherwise. The beat carries `SourceEpoch`, so a late subscriber learns it without asking.
- **A new `SourceEpoch`** means the publisher restarted and its data-stream sequence started again; a subscriber treats every instrument as Unknown.
- **The in-band side is market data.** What makes the state whole again (for a book: a clear, then every level) travels in the data stream in the publisher's own schema. SPP never carries it.

## 3. Evolution
1. **Append only.**
   - A new field goes at the end of a root block or group entry, with `sinceVersion`.
   - A reader uses the received `blockLength`: a field beyond it is absent and reads as 0 or empty.
   - A new group or data element goes after the existing ones, with `sinceVersion`.
2. **A new message is a new template id.** Ids 1-899 are core, assigned in this document. 900 is the `Extension` envelope.
3. **Unknown messages.**
   - A receiver that gets an unknown core template, or an `Extension` it does not implement with must-understand set, answers `Status{Unsupported}` echoing the `CorrelationId` (when it is not 0), and counts it.
   - An unknown `Extension` without must-understand is ignored.
   - Nothing unknown closes a connection.
4. **Newer is never an error.** The header `version` is the sender's version. A receiver must not reject a message for a higher version or a longer block; it reads what it knows. The framing in section 4 lets it skip a message whole.
5. **Breaking changes need a new `schemaId`.** These are: removing or retyping a field, reordering, or changing a meaning. SPP version 1 stays readable for as long as the release policy (section 6) says.

## 4. Bindings
Every binding frames each SPP message with a **u32 little-endian length** (the message's bytes, header included), so a reader can always skip a message it cannot decode. A frame may carry several messages back to back.

- **Aeron.**
  - One offer is one frame: one or more length-prefixed messages.
  - Request and reply use Aeron response channels. The requester subscribes to a response channel (`control-mode=response`), then publishes requests with `response-correlation-id` set to that subscription's registration id. The service replies on a response publication created with the requesting image's correlation id, so each requester receives only its own replies.
  - The service sizes its terms (`term-length`) to its largest reply.
  - A state stream is an ordinary publication, one frame per offer, with no response channel. Subscribers should be untethered, so a slow one cannot hold the publisher back; one that falls behind sees a gap and asks for `Republish`.
- **TCP.** A byte stream of length-prefixed messages. Each side sends `Hello` first. A peer that sends anything before `Hello` is treated as version 1 with no extensions. Specified now; implemented with the TCP recap component and the relay (planned).
- **Agent control socket** (apps to agent, `SOCK_SEQPACKET`). Extensions travel inside the agent control ops `RELAY` / `RELAY_REPLY` (planned), one SPP message per datagram after the control header.

## 5. Versions and `Hello`
- **When it is sent:**
  - Each side sends `Hello` when a connection, or an Aeron image, appears, and again after every reconnect.
  - On Aeron, the requester sends it as the first message on its request publication. The service answers with its own `Hello` on the response publication.
- **Agreeing a version:** both sides use `min(my ProtoVersion, peer ProtoVersion)`. If that is below either side's `MinProtoVersion`, the side that notices answers `Status{Unsupported}`, logs both ranges, and sends nothing else.
- **Before a `Hello` arrives,** a peer is assumed to speak version 1 with no extensions.
- **What the agreed version decides:**
  - A sender uses a new message or extension only with a peer whose `Hello` advertised it.
  - Fields appended to an existing message may always be sent, because older readers skip them.
- **State is per peer, never per process.** One agent talks to several gateways and snapshot services at different versions at once, and keeps each one's last `Hello`.
- **One-to-many streams** (a state stream) have no handshake. Every SPP header carries the sender's version, and the `SourceState` beat carries `SourceEpoch`, so every subscriber, late joiners included, learns both from the stream itself. Live data streams carry neither: they stay in the publisher's schema.

## 6. Deployment and upgrade
The intended order is **agents first, then gateways together with their snapshot services (one release), then apps last**. To make that safe:

| Component at release N | Must work with |
|---|---|
| agent | gateways and snapshot services N-1 and N; apps over a longer window, stated in the release notes |
| gateway, and its `-snap` | agents N-1 and N (agent fleets upgrade over days) |
| app | agent N and gateway N for new features; a fast `Unsupported` otherwise |

Consequences:
- A gateway rolled back after the agents were upgraded keeps working, because agents speak N-1.
- A new canonical template is mapped in agent config before a gateway sends it. An unmapped template is counted (`unknown_templates`) and dropped, never misrouted.
- Every entry in `docs/compat-log.md` states any upgrade-order constraint it adds.

## 7. Extensions
- **Ids:**
  - 0x0001-0x7FFF are assigned by subplane and listed in `docs/protocol/extensions.md`.
  - 0x8000-0xFFFF are private to a deployment and never assigned.
- **Bodies are opaque** to everyone except the extension's two endpoints. The owner chooses the encoding; its own SBE schema is recommended, designed with the same rules (the `subplane-schema-designer` skill helps).
- **Advertising:** a peer advertises each extension it implements, with a version, in `Hello`.
- **Flags:**
  - must-understand (bit 0): a receiver without the extension answers `Unsupported` instead of ignoring the message;
  - reply expected (bit 1): the receiver answers with an `Extension` or a `Status` echoing the `CorrelationId`.
- **App extensions through the agent (planned).** An app sends an `Extension` over its control socket. The agent forwards it to a service declared in its config, only for extension ids that the config allows and the service advertised, and relays the reply back. The agent routes on the envelope and never reads the body.

## 8. Not in SPP
- Market data, which stays on the bus.
- Agent to app delivery, which uses the ring and its control events (`docs/context/architecture.md`).
- The agent's own control socket operations (sessions, filters, snapshots), which have their own protocol and version (`CONTROL_PROTO_VERSION`).
