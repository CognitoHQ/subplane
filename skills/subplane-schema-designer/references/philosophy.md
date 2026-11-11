# Data messages: what subplane needs from a bus schema

subplane's agent sits on every application box. It subscribes to the bus once, finds message boundaries by walking the schema, decodes only routing fields, filters per app session, and copies the original bytes into each app's ring. Everything below follows from that: the cheaper a message is to route, the more apps a box can serve within its latency budget (tens of microseconds on kernel sockets).

## Routing fields
- **Instrument** (integer), **sequence** (u64, one counter per stream, only going up), **exchange timestamp** (ns since the epoch), **venue** (small integer).
- Put them in the **root block, at the same offsets in every data message**. The canonical schema uses one 40-byte prefix: `InstrumentId u32, VenueId u16, Flags u16, Sequence u64, InstrumentSeq u64, ExchangeTs u64, RecvTs u64`. `Sequence` counts the stream; `InstrumentSeq` counts the instrument across the topic (0 = not counted). Routing then reads at fixed offsets, whatever the template.
- The agent config maps them by name (`decoder.fields`). A field inside a group cannot be a routing field.

## Shape
- **Flat:** no repeating groups and no varData in hot data messages. A flat message is a fixed block, so the same bytes are a C++ struct, a NumPy dtype and a q record (`subplane schema --views`). The agent also finds the next message with one addition.
- **Naturally aligned:** each scalar sits at a multiple of its size at its payload offset (8-byte header plus the root offset). Then the generated views are plain structs, with no accessor fallback.
- **A deep book:** use fixed arrays for the top N levels (`Price10`, `Qty10` in canonical), and send the rest as updates. One level per `BookUpdate` keeps updates flat.

## Values
- **Prices and quantities:** `int64` fixed point, with one exponent for the whole schema (canonical uses 10^-9, declared as a constant). No floats, no exponent per message.
- **Instruments are integers.** Names arrive once, in a definition message, and the agent and apps map id to name. A string instrument can be routed, but it costs a hash per message.
- **Timestamps:** u64 ns since the Unix epoch, 0 when unknown.
- **Enums:** a small integer encoding, with every value named in the schema.

## One venue event, several messages
A venue update that changes several things at once (five book levels, a trade and the levels it consumed) becomes several flat messages, one per change, with the same `Sequence` order. An app that must not act on a half-applied event needs to know where the event ends:
- Mark the last message of the event. Use a bit in the prefix's `Flags` field, and name that bit in the schema's description.
- Defining a new `Flags` bit in a published schema is an evolution: it is additive, and it gets a compat-log row saying what readers that ignore it will see (the in-between states).
- Don't pack the event into one message with fixed arrays of "up to N" changes. It duplicates the single-change message, wastes space when fewer change, and fails when more do.

## State
- **A self-contained value** (definition, trading status, last trade, top of book quote): the newest message is the whole state. The agent's last-value cache can serve it to late joiners.
- **Built-up state** (a book from updates, accumulated volume, VWAP): no single message is the state. A late joiner needs recap from a snapshot service, so the topic's last-value cache should be off (`lvc: off`). Design these messages so a replica built from the stream is exact. For example, send a book `Clear` and full depth after a resync.

## Admin
- A **heartbeat** on idle streams. It should carry the publisher's identity (`SourceEpoch`, its start time) so subscribers can tell a restart; canonical v1's `Heartbeat` does not yet, and canonical v2 adding it is planned. It should also carry `StreamId` (the canonical `Heartbeat` has it: `topic_id << 16 | partition_num` of the stream it is sent on, 0 = unstamped), so the agent can count a publisher sending on the wrong stream.
- A **sequence reset** message when a publisher restarts its sequence on purpose.
- Admin messages are routed by topic, not by instrument. List their template ids with `--admin` when linting.

## What the agent never needs
- Fields beyond routing: apps decode those.
- Variable-length data in hot messages.
- Any message from an app: data flows one way, from the bus to the agent to the apps.
