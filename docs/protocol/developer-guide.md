# Developer guide: talking to subplane

For **gateway and snapshot-service developers**: you publish a bus and answer subplane's control messages. App developers: read `docs/guides/app-developer.md` instead.

The normative rules are in `subplane-protocol.md` (SPP). This guide is the walk-through. Items marked (planned) are specified but not built yet.

## The picture

Data flows one way, from the bus to the agent to the apps. Control is a few short conversations, always started by the side that needs something, and always in SPP when it crosses a process boundary between subplane services.

```mermaid
flowchart LR
    EX["Exchange<br/>(its own protocol)"]
    subgraph PUB["Publisher side: one release"]
        GW["Gateway<br/>spwsgw"]
        SNAP["Snapshot service<br/>spwsgw-snap"]
    end
    BUS(["Bus: canonical SBE<br/>Aeron IPC or UDP"])
    subgraph BOX["Application box"]
        AG["Agent<br/>subplaned"]
        APP1["App"]
        APP2["App"]
    end
    EX -- "WebSocket / REST" --> GW
    GW -- "live data" --> BUS
    BUS -- "live data" --> AG
    BUS -- "live data (silent client)" --> SNAP
    AG -- "ring (shm)" --> APP1 & APP2
    APP1 & APP2 <-. "control socket:<br/>sessions, filters, recap flag" .-> AG
    AG <-. "SPP: RecapRequest, reply<br/>(Aeron response channel)" .-> SNAP
    SNAP -. "SPP: Republish (planned)" .-> GW
    GW -. "SPP: SourceState, state stream (planned)" .-> SNAP
    APP2 -. "SPP Extension, relayed (planned)" .-> AG
```

**What travels where:**

| Path | Encoding | Carrier | Who starts it |
|---|---|---|---|
| Live data | the bus schema (canonical SBE, or a firm's own) | Aeron, UDP multicast, TCP | the publisher, continuously |
| Agent to app | the publisher's bytes plus a 64-byte header | shared-memory ring | the agent |
| App to agent | agent control protocol | Unix socket | the app |
| Agent to snapshot service | SPP | Aeron response channel (TCP planned) | the agent, when a session needs state |
| Snapshot service to gateway | SPP | Aeron (planned with feeds step 4) | the snapshot service, after losing state |
| Gateway to snapshot services | SPP `SourceState` | its own Aeron stream (planned with feeds step 4) | the gateway, on every change and once a second |
| App to service, through the agent | SPP `Extension` | control socket, then Aeron or TCP (planned) | the app |

## For gateway and snapshot-service developers

### 1. Publish the bus
- Publish messages in your schema, back to back per Aeron offer, framed by the schema.
- Every data message carries the routing fields at fixed root offsets: instrument, sequence, exchange timestamp, venue. The canonical schema puts them in one 32-byte prefix.
- Keep one sequence per stream that only goes up.
- Announce your identity: canonical v2's `Heartbeat` will carry `SourceEpoch` and your SPP version (planned).
- For a new schema, use the `subplane-schema-designer` skill.

### 2. Speak SPP
Use the generated codec (`include/subplane/proto/subplane_proto_codec.h`, namespace `sp_proto`), or generate one for your language from `schemas/subplane_proto.xml`. Put a u32 little-endian length before every message you send.

```cpp
uint8_t buf[256];
sp_proto::HelloEncoder hello(buf + 4, sizeof(buf) - 4);
hello.CorrelationId(0).PeerEpoch(start_ns).ProtoVersion(1).MinProtoVersion(1)
     .Role(sp_proto::Role::Snap).ServiceName("spwsgw-snap");
const uint32_t n = static_cast<uint32_t>(hello.finish());   // 0 means it did not fit
std::memcpy(buf, &n, 4);                                     // the binding's length prefix
publication.offer(buf, 4 + n);
```

- Answer every peer's `Hello` with your own.
- Remember the peer's version range and extensions, per peer.
- Answer anything you don't implement with `Status{Unsupported}`, echoing its `CorrelationId`.
- Never close a connection over an unknown message.

### 3. Serve recap (snapshot service)
The snapshot service subscribes to the live stream as a silent, untethered client, rebuilds each instrument's state with the publisher's own rules, and answers:

```mermaid
sequenceDiagram
    participant A as Agent
    participant S as Snapshot service
    A->>S: RecapRequest{CorrelationId=42, Instrument, LastSeqSeen}
    alt state is current
        S->>A: RecapBegin{42, AsOf=S, SourceEpoch}
        S->>A: RecapRecord{42, Payload = stream message} (repeated)
        S->>A: RecapEnd{42, RecordCount}
    else not ready (just started, or the stream had a gap)
        S->>A: RecapReject{42, NotReady}
    end
    Note over A: merges: snapshot as of S, then live above S
```

- **S is a live sequence:** the state contains every live message up to and including S.
- **Records are your stream's messages,** carried as opaque payloads. The agent delivers them to apps unchanged, flagged as snapshot.
- **Replies are contiguous:** don't interleave two replies on one response stream.
- **When to reject:** while your state for the instrument is unknown or stale, reject with `NotReady`. The agent retries with backoff.
- **Check your counts:** the agent drops a reply whose `RecapEnd.RecordCount` differs from the records it received, and asks again.
- **Agent side:** implemented. The agent sends `Hello` first on each connection, then one `RecapRequest` per instrument, and reads SPP replies (the partition's default, `recap.reply: spp`).

### 4. Handle republish (gateway, planned with feeds step 4)
`Republish{Instrument or 0 for all}` asks the gateway to publish the instrument's state again on the live stream: definitions, then a book clear and full depth. Answer with `Status{Ok}`, or `Busy` when rate-limited. Every subscriber sees the result in band, which is how a snapshot service recovers without a private channel.

Say when your state can be trusted with `SourceState` on a state stream of your own (a separate Aeron stream, never the data stream):
- `Stale` when you lose your upstream for an instrument (a gap, a disconnect), stamped with the last data `Sequence` you sent before it;
- `Ok` once you have republished the instrument whole in band (a book clear, then every level), stamped with the `Sequence` of the last message of that burst;
- a beat with `Instrument` 0 every second, carrying your `SourceEpoch`.

Your data stream stays pure market data in your schema. A snapshot service applies each state once its copy of the data stream reaches that `Sequence`, and answers recaps only for instruments that are `Ok`.

### 5. Add an extension (planned)
Pick an id (0x8000 and up for your own deployment; ask for one below 0x8000 in `extensions.md` if it is general). Advertise it in your `Hello`, and handle `Extension{ExtensionId, ExtType, Body}`. The body is yours; give it its own small SBE schema.

## For app developers

Apps never see SPP or Aeron: they read a ring and use the agent's control socket. Everything an app developer needs is in `docs/guides/app-developer.md`, including recap sessions and the events above from the app's side.

## Message map

| SPP message | Sent by | Answered with |
|---|---|---|
| `Hello` (1) | everyone, on connect and reconnect | `Hello` |
| `Status` (2) | anyone | |
| `RecapRequest` (10) | agent | `RecapBegin`, `RecapRecord`…, `RecapEnd`, or `RecapReject` |
| `Republish` (20) | snapshot service, operator | `Status` |
| `SourceState` (30) | gateway, on its state stream | nothing (one-to-many) |
| `Extension` (900) | app (through the agent), service | `Extension` or `Status`, if reply-expected |

| Ring event | Code | Means |
|---|---|---|
| `SYMBOL_STALE` | 0xA9 | your view of the symbol has a hole; a new recap has started |
| `RECAP_PENDING` | 0xAA | waiting for a snapshot of the symbol |
| `RECAP_BEGIN` | 0xAB | snapshot records follow, as of S |
| `RECAP_COMPLETE` | 0xAC | snapshot done; live continues from S+1 |
| `RECAP_FAILED` | 0xAD | this attempt failed; the agent retries |
