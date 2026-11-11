# Build a custom integration

Use this when no reference setup fits (`deploy-reference.md`): your own bus, your own schema, a feed subplane has no gateway for, or a new kind of data. Pick the section that matches what you are building. Each is self-contained.

| You are building | Section |
|---|---|
| the agent's connection to an existing bus (multicast, TCP, Aeron) | 1. Connect your own bus |
| a publisher or gateway that feeds subplane | 2. Publish into subplane |
| a new message type or schema | 3. Design a schema |
| a snapshot service, or a service apps can ask questions of | 4. Answer subplane's control messages |
| new agent code (a transport, a decoder) | 5. Extend the agent |

**What never changes:** the agent subscribes, routes and fans out. It decodes only routing fields and delivers the publisher's bytes unchanged, and it never publishes data. A custom integration configures that core or feeds it, never widens it.

## 1. Connect your own bus

A **topic** in the agent's config is a named feed that apps subscribe to; a **partition** is one stream of it, with one publisher and one sequence. Each topic needs an entry in the site's registry (`topic_name` to `topic_id`; `site/topics.example.yaml`), and the config has this shape:

```yaml
registry: site/topics.yaml        # or the entries inline: [ { id: 21, name: eq.nyse } ]
topics:
  - name: eq.nyse                 # the id comes from the registry; no `id:` here
    transport: { type: udp_multicast, group: 239.1.1.1, port: 30001 }
    framing: { ... }              # keys here are shared by the topic's partitions
    decoder: { ... }
    # partitions:                 # optional: one entry per stream, deep-merged over the keys above
    #   - {}
    #   - transport: { group: 239.1.1.2 }
```

A topic with no `partitions:` is one partition. Names use `[A-Za-z0-9._-]`, 1 to 48 characters, with `.` between segments (`eq.nyse`); sessions match them with `*` (one segment) and a final `>` (the rest). The id is assigned once by the site and never reused. Top-level `partitions:` is refused. For each partition (or the topic, when it has one) you answer four questions:

**a. Where is it?** (`transport`)

| Bus | `transport` |
|---|---|
| UDP multicast | `{type: udp_multicast, group: 239.1.1.1, port: 30001, interface: 10.0.0.5}` (`group: ""` for unicast) |
| TCP byte stream | `{type: tcp_stream, host: h, port: p, record_length: {offset: 0, size: 2, scale: 1, add: 0}}`: how to find each record's length in the stream |
| Aeron | `{type: aeron, channel: "aeron:udp?endpoint=..."}`; the stream id defaults to the partition's `stream_id` (`topic_id << 16 \| partition_num`), or set `stream_id: N` for a stream with another id; `driver: {embedded: true}` runs a media driver inside the agent |

**b. How are messages packed?** (`framing`)
- `packet_header`: fields at the start of each packet, for example a sequence number and a message count.
- `message_prefix`: fields before each message, for example a length.
- `length`: where each message's length comes from:
  - `message_prefix.<field>`;
  - `schema` (the default): computed from the SBE schema, so back-to-back messages need no prefix;
  - `packet`: one message per packet.
- `sequence`: where the gap-detection sequence lives:
  - `packet_header.<field>`;
  - `message.<SchemaField>`;
  - `none`.

```yaml
framing:
  packet_header: [{name: seq, type: u64le}, {name: count, type: u16le}]
  message_prefix: [{name: len, type: u16le}]
  sequence: packet_header.seq
  length: message_prefix.len
```

**c. Which fields route?** (`decoder`)
- `schema`: the publisher's SBE XML. It is the only hard requirement on a publisher.
- `fields`: the root-block field names holding the `symbol` (an instrument id or name), the `sequence`, the `exchange_ts` and the `venue`. Only these are decoded. A publisher that stamps its stream identity in a heartbeat (the canonical `Heartbeat` has `StreamId`) can add `stream_id: StreamId`; the agent then counts `stream_mismatch` when the stamp differs from the partition's `stream_id`.
- `messages`: template id to type, for example `{template: 1, msg_type: TRADE}`. Types are `TRADE`, `QUOTE`, `BOOK_UPDATE`, `BOOK_SNAPSHOT`, `OHLCV`, `STATUS` and `DEFINITION`. Administrative messages take `{template: 100, admin: HEARTBEAT}` (also `SEQ_RESET`). Unmapped templates are counted and skipped, never guessed.
- Not SBE? `decoder.type: dbn` (Databento) and `flatbuffers` exist too.

**d. Does it match the wire?** Before you start the agent, check against live packets:

```bash
build/tools/subplane probe --schema bus.xml --framing '{...}' --group 239.1.1.1 --port 30001 --packets 200
```

PASS means the template ids are in the schema, the lengths tile every packet exactly, and the sequence is monotone. Then start the agent and watch `GET /partitions` (named `<topic>/<n>`). `unknown_templates`, `undecodable_tail`, `gaps` and `malformed` all should stay at 0.

Every key: `docs/context/architecture.md`, "Configuration reference", "Transport", "Framing".

## 2. Publish into subplane

You are writing a feed handler, a gateway or a derived-data service. What the agent needs from you:
- **An SBE schema** that apps can decode. Either reuse the canonical schema (`schemas/subplane_canonical.xml`: Trade, Quote, BookUpdate, BookSnapshot, Ohlcv, Status, InstrumentDefinition, Heartbeat), or bring your own (section 3).
- **A topic.** Ask the site for your topic's name and id (an entry in the registry). Each stream you publish is a partition of it, numbered from 1, and its Aeron stream id is `topic_id << 16 | partition_num`.
- **A sequence per stream.** It must increase by one per message (or per packet) so that gaps are visible.
- **A heartbeat** when idle, so a silent stream is not mistaken for a dead one.
- **Whole messages per send.** Several messages back to back per datagram or offer are fine; the agent walks them apart with the schema.

Then point a topic at your stream (section 1).

For exchange feeds over WebSocket, write a pack for the existing gateway instead of a new process (`subplane-feeds`, `docs/pack-format.md`): one YAML file per exchange maps JSON fields to canonical messages.

## 3. Design a schema

Rules that keep the agent fast and apps simple:
- **Routing fields** (symbol, sequence, exchange time, venue) go in the root block, at the same offsets in every message.
- **Hot messages are flat and naturally aligned.** Groups and variable-length data belong in reference or snapshot messages.
- **Prices** are `int64` fixed point; **instrument ids** are integers.
- **Evolve by appending** fields (`sinceVersion`). Never move or remove a field; a breaking change needs a new schema id.

Lint before you publish:

```bash
build/tools/subplane schema --schema yours.xml --lint --routing InstrumentId,Sequence,ExchangeTs,VenueId
build/tools/subplane schema --schema yours.xml --lint --against previous.xml   # only allowed evolution
```

To generate typed views (C++, NumPy, q) for apps: `subplane schema --schema yours.xml --views all --out dir`. The `subplane-schema-designer` skill (`skills/`) guides a coding agent through the same rules.

## 4. Answer subplane's control messages

A **snapshot service** gives the agent current state for late joiners, so apps start from a snapshot instead of asking your publisher. A **service** can also answer questions apps send through the agent (planned). Both speak the subplane protocol (SPP), an SBE schema of its own. A publisher that recovers from its own upstream (a gateway resyncing an exchange) says when its data can be trusted with SPP `SourceState` on a state stream of its own; its data stream stays pure market data in its schema. The walk-through with diagrams is `docs/protocol/developer-guide.md`; the normative spec is `docs/protocol/subplane-protocol.md`.

## 5. Extend the agent

New transports, decoders or fan-outs are compiled-in components chosen by config (no plugins, no `dlopen`). Read `docs/context/invariants.md` first: the hot path has no heap, no mutex, no I/O and one virtual call per packet batch. Then read `docs/context/architecture.md` ("Component registry", "Source layout") and `CONTRIBUTING.md`.
