# The subplane protocol (SPP)

Version 1. Status: specified. Implemented: the codec (`include/subplane/proto/subplane_proto_codec.h`) and the agent's side of recap over Aeron (`Hello`, `RecapRequest`, the reply messages, `Status`; see `docs/context/architecture.md`, "Recap"). Planned: the gateway's `Hello` and `Republish` (feeds step 4), the TCP binding, and the app extension relay.

SPP is how subplane processes talk to each other about control:

| From | To | What |
|---|---|---|
| agent | snapshot service (`<service>-snap`) | recap requests and replies |
| snapshot service | gateway | republish requests |
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
| 10 | `RecapRequest` | agent to snap | The state of `Instrument` (and `Venue`) as of a sequence. Carries `AgentEpoch`, `LastSeqSeen` and `SourceEpochSeen`. |
| 11 | `RecapBegin` | snap to agent | Opens a reply: the records that follow are the state as of `AsOf` (S), built from publisher `SourceEpoch` |
| 12 | `RecapRecord` | snap to agent | One record: `Payload` is a stream message in the stream's own encoding, bytes as the publisher sent them |
| 13 | `RecapEnd` | snap to agent | Closes a reply, with the number of records sent |
| 14 | `RecapReject` | snap to agent | Replaces a reply. `Reason`: Other, NotReady, UnknownInstrument, Busy. |
| 20 | `Republish` | snap (or operator) to gateway | Publish `Instrument` (0 means every instrument) again, on the live stream. Answered by `Status`. The data itself appears in band. |
| 900 | `Extension` | either way | An envelope for an extension: `ExtensionId`, `ExtType`, `Flags` (bit 0 must-understand, bit 1 reply expected), and an opaque `Body` |

**A recap reply** is `RecapBegin`, then zero or more `RecapRecord`, then `RecapEnd`, all with the request's `CorrelationId`. It is sent contiguously: replies to different requests are not interleaved. A service that cannot answer sends a single `RecapReject` instead. S must be in the same sequence space as the live stream. Records carry the stream's bytes untouched, so recap works for any stream encoding (SBE, FlatBuffers, DBN), and apps receive the publisher's bytes unchanged.

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
- **One-to-many live streams** have no handshake. Canonical schema version 2 (planned) carries the publisher's `SourceEpoch` and SPP version in its `Heartbeat`, so every subscriber, late joiners included, learns them from the stream it already reads.

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
