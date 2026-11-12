# Architecture: subplane

Status key: everything below is implemented unless marked "(planned)". The code is the source of truth; where a header comment and this document disagree, fix this document.

## Vocabulary

Three words, three lifecycles. They are never interchangeable.

| Term | What it is | Lifecycle |
|---|---|---|
| **component** | Code compiled into the binary and registered in a static table: a transport, a framer, a decoder, a fan-out. | Fixed at build time. |
| **activation** | One partition of a topic in YAML (an entry under a topic's `partitions:`, or the topic itself when it has none). It selects components by `type:` and parameterizes them: transport subscription, framing block, decoder model and schema file. | Chosen at startup. Changing it requires a restart. |
| **session** | A runtime app subscription: a filter table plus a shared-memory ring. | Created and destroyed at runtime, by YAML (`owner=config`) or by an app over the control channel (`owner=client`). Reloadable. |

Operators may still say "plugin" for an activation. In code, a "partition" is one activation. A partition belongs to a **topic**, the name apps subscribe by; see "Topics and partitions".

## Topics and partitions

A **topic** is a named feed; it holds one or more **partitions**, numbered from 1. Each partition is one stream with one publisher and one sequence. Partition is a publishing construct, not a data-model one: apps see topics and never partitions. Partition detail (streams, sequences, gaps, transport) is for the operator, in `/partitions`, the metrics and `/topology`.

| Term | Form | Who assigns it |
|---|---|---|
| `topic_name` | string of `[A-Za-z0-9._-]`, 1 to 48 characters, `.` between segments (Kafka-legal) | The site (policy). |
| `topic_id` | u16, 1 to 32767; 0 is reserved | The site, once, in the registry file. Never reused or renumbered. |
| `partition_num` | u16, 1 to 65535, within a topic | The publisher's config. |
| `stream_id` | i32 = `topic_id << 16 \| partition_num`, always positive | Derived. It is the default Aeron stream id. |

The agent's name for a partition, in logs, metrics and HTTP, is `<topic_name>/<partition_num>`.

**Registry.** `topic_name` to `topic_id` is a site-wide file that every agent and every publisher reads, so an id means the same on every box, in every record and in every recording. `registry:` in the agent config is the file's path (relative to the config file) or the entries inline, a sequence of `{id, name}`. A bad name, an id outside 1 to 32767 or a duplicate id or name stops the agent at startup, as does a topic that is not in the registry, listed twice, or given an `id:` key. `site/topics.example.yaml` is the example the repository's `config.yaml` uses.

**Config shape.** Top-level `partitions:` no longer exists; the agent refuses it with an error naming `topics:`.

```yaml
registry: site/topics.yaml           # or inline: [ { id: 11, name: crypto.binance-spot } ]
topics:
  - name: eq.sample                  # the id comes from the registry; a topic has no `id:` key
    transport: { type: udp_multicast, group: 239.1.1.1, port: 30001 }
    framing: { ... }                 # keys at the topic level are shared by its partitions
    decoder: { ... }
    partitions:                      # optional; omitted means one partition (number 1)
      - {}                           # partition 1: no overrides
      - transport: { group: 239.1.1.2 }   # partition 2: deep-merged over the topic's keys
```

Each `partitions:` entry is deep-merged over the topic's keys: maps merge, anything else replaces, and `{}` means no overrides. A partition entry has no `name`; its number is its position, from 1. An Aeron transport without `stream_id` gets the derived `stream_id`; an explicit one is kept. A topic-level `transport.driver` (embedded media driver) goes to partition 1 only, and the other partitions use that driver; a partition entry may still set its own `driver:` for a different `aeron_dir`. Two partitions on the same transport address are refused (`read the same stream`). The limit of 64 partitions per agent still holds.

**Thread groups.** `agent.thread_groups[].topics` lists topic names; `partitions:` there is an error. A topic's partitions always share one group, and each topic must be in exactly one group.

**Session patterns.** A session's `topics:` lists patterns that match topic names by `.` segment: `*` matches exactly one segment, a final `>` matches one or more, anything else matches itself (`crypto.*`, `crypto.>`). An omitted or empty list means all topics. A pattern that matches no topic is an error at session create, and a session's topics cannot change after creation (`SET_FILTER` refuses). A session `partitions:` key is an error. As before, a session configuration error is logged and the agent keeps running.

**In the data path.** The record header carries `topic_id` (offset 48), and the session filter accepts by `topic_id` (a 32768-bit bitmap). A data record's `sequence` is the instrument's when the topic maps one (below), otherwise the stream's own, one per partition. Sequences in events (`GAP_DETECTED`, `TOPIC_*`, `TRANSPORT_*`, `SYMBOL_STALE.last_seq`, recap `as_of_seq`) are the stream's.

**Instrument sequence.** A publisher can count each instrument across the whole topic: 1, 2, 3 ... with no holes, whichever partition carries the record (the canonical `InstrumentSeq`, CME's `RptSeq`; 0 = not counted). The decoder routing field `instrument_sequence` (`fields: { ..., instrument_sequence: InstrumentSeq }`, `sbe_routing` only, data messages only, needs `symbol_id_mode: table`) turns on, for that partition, a per-instrument check after the stream's own:
- It keeps the last instrument sequence per symbol in an array owned by the thread group, sized `agent.max_symbols`, one per topic and shared by the topic's partitions (8 bytes per symbol per topic).
- The first record of an instrument syncs. `last + 1` is delivered. A record at or below `last` is a duplicate: counted in `inst_dups` and dropped. This is how the same record arriving on two partitions, or replayed, reaches an app once.
- A jump forward is delivered, counted in `inst_gaps`, and preceded by `INSTRUMENT_GAP{symbol_id, topic_id, expected, received}` on the sessions that take the record. On a recap topic the recap sessions that take the symbol get `SYMBOL_STALE{reason 5}` and a recap of that symbol only.
- A record at or below `last` is a duplicate however far behind it is (no `dup_window` here): one partition may lag another by any amount. An instrument's count starts again only with its stream: when the stream restarts (`BUS_SEQ_RESET`, a backward stream jump beyond `dup_window`, `TRANSPORT_UP{publisher restart}`), every instrument of the topic syncs on its next record.
- The record's header `sequence` is the instrument sequence (0 when the record carries none), so an app orders, de-duplicates and finds gaps per symbol by `sequence` whichever partition delivered it. Snapshot records of a recap carry the instrument's sequence as of S, read from the record itself; the held live records follow it.
- The stream sequence still finds transport loss: `gaps`, `GAP_DETECTED`, and the recap floors and merge (the hold keeps each record's stream sequence beside it).
- Measured: no cost above the bench's run-to-run noise (`replay --bench`, canonical topic, mapped against unmapped).
- Not yet: filling an instrument gap from another partition (make-before-break moves, planned) and a late record behind its instrument is dropped, not reordered.

**Stream identity in band.** The canonical `Heartbeat` (template 100) carries `StreamId` (int32, root offset 40, 0 = unstamped). The decoder routing field `stream_id` (`fields: { ..., stream_id: StreamId }`) reads it from admin messages only. When a heartbeat's stamp is non-zero and differs from the partition's `stream_id`, the partition counts `stream_mismatch` and records `last_stream_stamp`; data is not dropped. It catches a publisher stamped for one stream sending on another.

**Restart rule.** Only `sessions:` is reloaded. `POST /reload` answers `{"status":"reloaded","ignored":["topics"]}` when `registry`, `topics` or `agent.thread_groups` changed, and logs `RELOAD_IGNORED`; those edits take effect on restart. Adding a partition therefore takes a restart; the weekend practice is to add partitions on the primary and the DR agent, then move tickers.

**Planned, not built:** a resolver (the SPP `Resolve` op served by the snapshot service), make-before-break intraday moves of a ticker between partitions, a gateway publishing one topic over several partitions, and a Kafka ingest transport.

## Deployment

One `subplaned` process (the agent binary) per application box, next to the apps it serves. It joins the multicast group once; apps do not. It runs as an ordinary Linux process (C++26, GCC 16, Ubuntu 26.04; INV-007). Apps and agent share `/dev/shm` (ring files) and a Unix socket path (control channel). Nothing leaves the box except the bus subscription.

```
bus (UDP multicast) -> [ partition thread group ] -> ring per session (/dev/shm) -> apps
                          poll, frame, decode,        ^
                          sequence, LVC, filter       | control socket (SEQPACKET), HTTP API (cold)
```

## Data flow

Per thread group, per loop iteration, for each partition in the group:

1. `Transport::poll(batch)` returns up to `batch` packets (`recvmmsg`, non-blocking).
2. Per packet: `Framer::parse` reads the packet header fields. If the framing yields a packet-level sequence, the sequence check runs here and a duplicate packet is dropped.
3. Per message: `Framer::next` yields the next SBE message and its length (from a prefix, or by walking the schema).
4. `SbeRoutingDecoder::decode` fills a `TickHeader` (symbol id, msg type, venue, sequence, exchange timestamp, template and schema ids). If the sequence lives in a message field, the sequence check runs here.
5. DATA messages update the last-value cache, then for each session attached to the partition: `FilterTable::accept(hdr)`, then `RingWriter::publish(hdr, bytes, len)`. Rejected messages count as `filtered`.
6. BUS_ADMIN messages (heartbeat, sequence reset, snapshot markers, trading status) are published in-band to sessions that accept bus admin. A SEQ_RESET also resets the expected sequence.
7. Control events generated by the agent (gap, degraded, recovered, filter reloaded, restarted, session closing, symbol mapped) are published to sessions that accept agent control, in the same ring and in order with the data.
8. After polling: heartbeat-timeout check, apply pending session commands, ring heartbeats (about every 1 ms).

Payload bytes are copied once, from the receive arena to each accepting ring.

## Component registry

`registry/component_registry.h`: explicit static tables, no self-registering statics. The table lives in `src/registry/builtin_components.cpp` and is greppable. Only the transport table exists today: `udp_multicast` (and `udp` as an alias, unicast when `group` is empty), `tcp_stream` and `aeron` (present when built with `SUBPLANE_WITH_AERON`, the default). An unknown `type:` is a startup error that lists the available types.

The framer and the decoder are not looked up through the registry. The framer is built from the `framing:` block, and `decoder.type` accepts only `sbe_routing`. Decoder and derivation tables are planned.

## Transport

`transport/transport.h` defines the interface: `open` (cold), `poll(PacketBatch&)` (hot), `close`. The hot-path contract is one virtual call per packet batch. Packet views point into transport-owned memory valid until the next `poll`; there is no allocation after `open`.

`udp_multicast` (`UdpMulticastTransport`):

- Non-blocking UDP socket, `SO_REUSEADDR`, optional `SO_REUSEPORT`, `IP_ADD_MEMBERSHIP` on the configured interface address. An empty `group` means plain UDP unicast bind (used for CI and local demos).
- `recvmmsg(MSG_DONTWAIT)` into one arena of `batch * mtu` bytes allocated in `open()`. `batch` is 1 to 64.
- `SO_RCVBUF` from `rcvbuf`; the kernel may clamp it to `net.core.rmem_max` and the effective size is available via `effective_rcvbuf()`.
- `SO_BUSY_POLL` when `busy_poll_us > 0`. If the kernel or driver refuses it, the transport continues without it.
- `SO_TIMESTAMPNS` kernel receive timestamps when `kernel_timestamps: true` (default off). Otherwise the receive timestamp is taken after `recvmmsg` returns.
- `multicast_loop` (default true) is needed for same-host tests.

`tcp_stream` (`TcpStreamTransport`): a byte stream reassembled into whole records, each handed to the pipeline as one packet. Encoding-blind: the record-length rule and the optional stream prelude are configuration.

```yaml
transport:
  type: tcp_stream
  host: 127.0.0.1
  port: 9550
  record_length: { offset: 0, size: 1, scale: 4, add: 0 }   # total bytes = little-endian field * scale + add (size 1, 2 or 4; add: 4 for a size prefix that excludes itself)
  prelude: dbn            # none | dbn: "DBN" u8 version u32 len, then len bytes, skipped once per connection
  rcvbuf: 4194304         # reassembly arena bytes (also SO_RCVBUF); at least 2 * max_record
  batch: 64               # records per poll
  max_record: 65536
  reconnect_ms: 1000      # 0 = peer close or error is fatal (poll returns -1)
  connect_timeout_ms: 2000
```

- Non-blocking connect completed inside `poll()`. `poll()` reads once into one arena allocated in `open()`, then cuts complete records (up to `batch`) as views into the arena; a record split across reads waits for the next poll. One clock read per batch.
- Peer close or a socket error closes the connection; with `reconnect_ms > 0` the transport reconnects with that backoff and `poll()` returns 0 meanwhile, so the partition reports DEGRADED through the normal heartbeat timeout. A record length of 0 or above `max_record` means the stream is misaligned: counted as a framing error and reconnected. A zstd-compressed DBN stream is fatal with a message (decompress with the `dbn` CLI).
- `subplane dbn-sim` serves DBN over TCP the way Databento's live gateway does after `Start()` (prelude, metadata, records) from an uncompressed `.dbn` file or synthetically; see Tools.

`aeron` (`AeronTransport`): one Aeron stream (IPC, UDP unicast or multicast) through the Aeron C client; every reassembled message is one packet to the pipeline. Encoding-blind: the message is whatever the publisher offered.

```yaml
transport:
  type: aeron
  channel: "aeron:ipc"          # or aeron:udp?endpoint=0.0.0.0:40456 | aeron:udp?endpoint=239.1.1.1:40456|interface=10.0.0.1
  stream_id: 720897             # optional: defaults to the partition's stream_id (topic_id << 16 | partition_num)
  aeron_dir: ""                 # media driver directory; empty = Aeron's default (/dev/shm/aeron-<user>)
  batch: 32                     # fragments per poll, 1 to 64
  max_message: 65536            # arena slot bytes; a larger message is counted (oversize) and dropped
  driver_timeout_ms: 10000      # wait for a live driver on open
  driver: { embedded: false, threading_mode: shared, dir_delete_on_start: true }   # optional in-process media driver
```

- The media driver is normally its own process (`aeronmd`, built alongside the agent). `driver.embedded: true` runs one inside the agent on its own threads (`shared`, `shared_network` or `dedicated`), for development, tests and single-box setups; the pipeline thread never runs the driver duty cycle, and the client conductor has its own thread too.
- `poll()` calls `aeron_subscription_poll` with a fragment assembler (messages larger than the MTU arrive whole) and a fragment limit equal to `batch`. The handler copies each message into the next slot of one arena of `batch * max_message` bytes allocated in `open()`, because Aeron's log-buffer bytes are only guaranteed while the handler runs and the pipeline reads the batch after `poll()` returns. One clock read per poll is the receive timestamp. No allocation, no mutex on the poll thread (the C client's image list is lock-free).
- Framing: one message per packet, so `length: packet` (or the schema default) with `sequence: message.<Field>` when the publisher stamps a sequence, or `none`. Liveness is the partition's heartbeat timeout as for any transport; image arrivals and departures and client errors are counted on the transport.
- Measured on the dev host (GCC 13, unpinned, embedded driver, `aeron:ipc`, 200k canonical messages offered back to back): 0 gaps, pipeline `recv_to_publish` p50 3.1 µs, p99 12 µs. That is poll to ring; the Aeron hop (offer to poll) is in front of it.
- `subplane aeron-pub` is the test peer and proof: canonical Trade/Quote/Heartbeat over an exclusive publication, with an optional embedded driver. `ctest -R aeron` runs it through the agent to `views_app` over IPC (publisher attached to the agent's embedded driver) and over UDP loopback (two drivers).

## Framing

`framing/framing.h`. The only hard requirement on a publisher is its SBE schema; everything else about packet layout is a declarative `framing:` block compiled at startup into fixed offsets. The hot path has no allocation and no virtual calls.

```yaml
framing:
  packet_header:                  # optional; fields at fixed offsets from packet start, in order
    - { name: seq,   type: u64le }
    - { name: count, type: u16le }
  message_prefix:                 # optional; fields before each message
    - { name: len,   type: u16le }
  sequence: packet_header.seq     # packet_header.<f> | message.<SchemaField> | none
  count:    packet_header.count   # optional; bounds the message loop
  length:   message_prefix.len    # message_prefix.<f> | schema (default)
  length_includes_prefix: false
  keep_prefix: false              # true: deliver each message with its prefix (whole unit, e.g. size-prefixed FlatBuffers)
```

- Field types: `u8`, `u16le`, `u16be`, `u32le`, `u32be`, `u64le`, `u64be`, `bytes` (needs `size`). An optional `offset:` overrides the running offset.
- **Length.** With a prefix, length comes from it. Without one (`length: schema`, the default), the length is computed by walking the schema: root `blockLength` from the SBE header, then each group (dimension composite gives `blockLength` and `numInGroup`, recursive up to depth 4) and each varData (length prefix). The walk is compiled per template at schema load into a small op array, so `[sbe][sbe][sbe]` back-to-back packets parse with no framing at all. The walk honours SBE's extension rule: a group or varData whose `sinceVersion` is newer than the `version` in the message header is not on the wire and is skipped. The walker and this framing block are validated against real CME MDP 3.0 packets (wire versions 6 and 9, schemas v9 and v13) by `tests/integration/test_cme_probe.sh` after `scripts/fetch-cme-sample.sh`; the MDP 3.0 block is `packet_header: [seq u32le, sending_time u64le]`, `message_prefix: [len u16le]`, `length_includes_prefix: true`.
- **Unknown template.** If the length is known (from a prefix, or the template is in the schema so the walk works) but the template has no mapping in `decoder.messages`, the message is skipped and `unknown_templates` is counted, with the last id kept in `last_unknown_template`. If there is no prefix and the template is absent from the schema, the length cannot be computed: the rest of the packet is dropped and `undecodable_tail` is counted. It is never guessed. A schema id mismatch in the SBE header is counted separately as `schema_mismatch`. Truncated or inconsistent messages count as `truncated`, `malformed` or `decode_errors`. Nothing is silent. The operator fix for a new template is the publisher's newer XML plus a restart.
- **Length `packet`.** `length: packet` makes the whole packet (after any packet header) one message; the decoder validates the length it finds inside. This is the default for the `dbn` and `flatbuffers` decoders, whose units carry their own length and arrive one per packet (or one per `tcp_stream` record). It cannot be combined with `message_prefix`.
- **Keep the prefix.** By default a `message_prefix` is stripped and the message handed to the decoder (and to the ring) starts after it, which is what SBE buses want. `keep_prefix: true` delivers each message from the start of its prefix with the prefix counted in its length. A size-prefixed FlatBuffer is aligned from its prefix and is only decodable whole, so a datagram of `[u32 size][flatbuffer]...` is framed with `message_prefix: [{name: size, type: u32le}]`, `length: message_prefix.size`, `keep_prefix: true` and each ring payload is the complete buffer the publisher finished.
- **Sequence sources.** `packet_header.<field>` (one sequence per packet, checked before the messages), `message.<SchemaField>` (each message carries it, mapped through `decoder.fields.sequence`), bare `message` (the decoder fills `TickHeader::sequence` from a format-defined field, as `dbn` does), or `none`. With `none`, gap detection is off for that partition and liveness is by heartbeat timeout only. (Planned: the same three sources for every routing field and for the extension tuple; see the vision roadmap, item 1.)
- **Encoding is per-partition configuration.** Transports are encoding-blind; the decoder component chosen by `decoder.type` sets `wire_format` on every record. Three decoders exist: `sbe_routing`, `dbn` and `flatbuffers`. `length: schema` is valid only for a decoder that can compute message length (SBE); other encodings use `length: packet` or a length prefix, and the agent rejects an invalid framing/decoder pair at startup (`"framing.length is schema but this decoder cannot compute message length"`). The decoder is held by value in the partition and selected with a switch per message, not a virtual call. The payload delivered to a ring is always the publisher's complete framing unit, never a slice of one. Planned: protobuf, generic fixed-offset layouts.
- **`subplane probe`** joins a group (or binds a unicast port), captures N packets and runs the production framer and schema walker over them. It checks that template ids are in the schema, that lengths tile each packet exactly, and that the sequence is monotone, and prints a verdict and the message mix. Exit 0 pass, 1 fail, 2 no packets or usage error.

## DBN decoder

`decoder/dbn_decoder.h`. Databento Binary Encoding: every record starts with a fixed 16-byte header (`u8 length/4`, `u8 rtype`, `u16 publisher_id`, `u32 instrument_id`, `u64 ts_event`), so routing needs no schema. `symbol = instrument_id` (mode `passthrough` by default, `table` remaps densely), `venue = publisher_id`, `exchange_ts = ts_event`, `template_id = rtype`, `wire_format = DBN (31)`, `sequence` from the per-rtype offset where DBN carries one (MBO at 52; trade, MBP-1 and MBP-10 at 44). `messages:` maps rtype names (`MBO`, `MBP_0`/`TRADE`, `MBP_1`, `MBP_10`, `CMBP_1`, `BBO_1S`, `BBO_1M`, `CBBO_1S`, `CBBO_1M`, `OHLCV_1S/1M/1H/1D`, `STATUS`, `DEFINITION`, `IMBALANCE`, `ERROR`, `SYMBOL_MAPPING`, `SYSTEM`, `STATISTICS`, or a number) to `msg_type` or `admin`; when omitted the default is MBO to BOOK_UPDATE, MBP_0 to TRADE, MBP_1/CMBP_1/BBO/CBBO to QUOTE, MBP_10 to BOOK_SNAPSHOT, OHLCV to OHLCV, STATUS/STATISTICS/IMBALANCE to STATUS, SYSTEM to admin HEARTBEAT; DEFINITION, SYMBOL_MAPPING and ERROR are unmapped (counted in `unknown_templates`) unless mapped explicitly. The record's own length byte must equal the packet length, else `decode_errors`.

Two facts learned from a real file (CLX5 MBO, 38,212 records): DBN's `sequence` is the venue's packet sequence, shared by every record from the same exchange packet, so `sequence: message` drops siblings as duplicates; leave the default `sequence: none` for DBN. And `symbol_id_mode: passthrough` needs `agent.max_symbols` above the largest instrument id (CME ids are in the hundreds of thousands), which makes the dense last-value cache and filter bitsets large (about 3.9 GB RSS at `max_symbols: 1048576`); a sparse LVC for passthrough ids is on the backlog.

On a passthrough partition, session `symbols:` entries are raw instrument ids used as-is; a session may not mix passthrough and table partitions.

## FlatBuffers decoder

`decoder/flatbuffers_decoder.h`, `decoder/flatbuffers_wire.h`. Routing fields are read straight off the FlatBuffers wire format by field id: root offset, signed offset to the vtable, u16 slot per id, uoffsets to nested tables and strings, every load bounds-checked against the buffer. No FlatBuffers library, no generated code and no schema file are needed in the agent: YAML names the ids, which come from the `.fbs` (declaration order, or explicit `(id: n)`). A field whose slot lies beyond the producer's vtable (added in a newer schema) reads as absent, value 0.

```yaml
decoder:
  type: flatbuffers
  size_prefixed: true                  # default: buffer starts with u32 size (flatc FinishSizePrefixed)
  symbol_id_mode: passthrough          # passthrough (integer symbol) | table (integer or string symbol)
  symbol_scope: global                 # global | venue (table mode, integer symbol, needs a venue path)
  dispatch: { type_field: { path: [2], type: u8 } }   # or `dispatch: identifier` (4-byte file identifier)
  messages:
    - { type: 1, msg_type: TRADE,                     # `identifier: "TRDE"` with identifier dispatch
        symbol: { path: [3, 0, 0], type: u32 },       # field ids root-first; all but the last are table fields
        venue:  { path: [3, 0, 1], type: u16 },
        exchange_ts: { path: [1], type: u64 },
        sequence:    { path: [0], type: u64 } }
    - { type: 3, admin: HEARTBEAT }
```

- **Message type.** FlatBuffers has no message header. `dispatch: type_field` reads a scalar in the root table, typically a union's `_type` field (the union value is the next id), and matches `type:`; `template_id` is that value. `dispatch: identifier` matches the 4-byte file identifier after the root offset; `template_id` is the entry's 1-based position in `messages`. An unmatched message counts as `unknown_templates` with the value seen in `last_unknown_template`.
- **Delivered unit.** The whole buffer, size prefix included when `size_prefixed` (default). A sub-table is not decodable alone (offsets, vtables and shared strings point outside it) and the producer aligned the buffer from its prefix, so apps call `GetSizePrefixedRoot` on the payload. One buffer per packet is the default framing (`length: packet`); several per datagram need `message_prefix` with `keep_prefix` (see Framing); over `tcp_stream` use `record_length: {offset: 0, size: 4, scale: 1, add: 4}`.
- **Checks.** A size prefix that does not account for exactly the rest of the unit is `decode_errors` (misaligned stream or wrong framing); a root offset outside the buffer likewise; a unit shorter than a root offset plus a table is `truncated`. `wire_format = FLATBUFFERS (32)`, `schema_id = 0`.
- **Fixture.** `tests/fixtures/flatbuffers/` holds real `flatc` output (a bus-like schema with a union body, the instrument two tables deep, an old-producer variant with a short vtable) as a `.bin`, a pcap and the expected routing fields; `ctest -R flatbuffers` runs the unit tests and the UDP replay through the agent. Measured on that fixture: about 57 ns per message for dispatch plus four routing fields, bounds-checked, on the dev host.

## SBE routing decoder

`decoder/sbe_schema.h`, `decoder/sbe_routing_decoder.h`. The schema XML (FIX SBE 1.0/2.0) is parsed once with vendored pugixml. Header layout comes from the schema's `headerType`, not an assumption. At init the decoder builds a dense by-template table of field offsets.

What it decodes, from the message root block only:

- `symbol` (integer field, or a `char[N]` field) interned to a dense `symbol_id`. Mode `table` (default) maps through the lock-free `SymbolTable`; mode `passthrough` uses the raw integer as the id and requires an integer field.
- `symbol_scope` (`global` default | `venue`): with `venue`, an integer symbol is interned together with the message's venue, so instrument 1 from venue 10 and instrument 1 from venue 20 are two symbols. Needs `symbol_id_mode: table`, an integer symbol and a mapped venue (the same option exists for `dbn`, where the venue is `publisher_id`, and for `flatbuffers`, where every message with an integer symbol needs a venue path). Session filters, `SYMBOL_MAPPED` names and `GET /lvc/{symbol}` then spell these symbols `<venue>:<id>` (`10:1`). Opt-in because it changes how filters are written; with globally unique ids (a site instrument master) `global` is enough.
- `sequence`, `exchange_ts`, `venue` (integer fields; venue is truncated to 16 bits).
- `stream_id` (integer field, read from admin messages only): the stream identity a publisher stamps in its heartbeat; see "Topics and partitions".
- `instrument_sequence` (integer field, read from data messages only): the publisher's count of the instrument in the topic; see "Topics and partitions", instrument sequence.
- Template id, schema id and schema version from the SBE header, and `msg_type` from the config mapping.

What it does not do:

- It never decodes groups, varData or any other field; apps get the original bytes.
- A routing field inside a group or varData is a startup config error. A field missing from a template is a warning and the value is 0. A field beyond a smaller `blockLength` (older publisher version) reads as absent.
- Known limitation (found on CME MDP 3.0): incremental refresh messages carry `SecurityID` only inside the `NoMDEntries` group, and one message can update several instruments. Such a bus can be routed by message type today (no `symbol` mapping), not by symbol. Per-entry symbol routing is on the roadmap.
- Only `type: sbe_routing` exists. Messages are mapped by template id to a data type (`TRADE`, `QUOTE`, `BOOK_UPDATE`, `BOOK_SNAPSHOT`, `OHLCV`, `STATUS`, `DEFINITION`) or an admin type (`HEARTBEAT`, `SEQ_RESET`, `SNAPSHOT_START`, `SNAPSHOT_END`, `TRADING_STATUS`); exactly one of `msg_type` or `admin` per entry. The admin names `RECAP_BEGIN`, `RECAP_END` and `RECAP_REJECT` map recap reply markers and take a `fields:` block (see "Recap"); `sbe_routing` and `flatbuffers` accept them, `dbn` rejects them because DBN's record types are Databento's fixed set.
- Known cost: symbol resolution goes through a `SymbolResolver` virtual call per decoded message. This is an exception to the one-indirect-call-per-batch rule, marked TODO in the header.

## Pipeline threading

One pipeline (`Pipeline`) per **thread group**. A group is one busy-polling thread, optionally pinned to a CPU, that owns a disjoint set of topics (all of each topic's partitions) and the sessions that subscribe only to them. A session whose topics span two groups is rejected. Default is one group containing every topic. Why: rings are single-writer, last-value slots are single-writer seqlocks, and control events must be ordered relative to data on each ring.

The cold side (config, control channel, HTTP) never touches pipeline state directly. It posts commands to the group through a fixed-capacity SPSC queue (1024 entries) and waits for the ack:

- `ADD_SESSION`, `REMOVE_SESSION`, `SWAP_FILTER`, `STOP`.
- **Filter swap (cheap RCU).** The cold side builds a new `FilterTable`, posts `SWAP_FILTER`, and the pipeline thread stores the new pointer (release) and acks a generation. The old table is freed only after the ack. The pipeline thread publishes `FILTER_RELOADED` in-band at the swap point. No mutex on the hot side.
- **Heartbeat.** The loop stamps each ring's `heartbeat_ns` about every 1 ms, and once on session add, so a reader attaching immediately does not see a zero heartbeat.
- The command queue has a single producer, serialized by the session manager's mutex.

The pipeline thread takes its clock after polling and guards timeout arithmetic with `now > last_rx`, because packets stamped during the iteration are later than a pre-poll clock and the unsigned subtraction would wrap.

## Sequence tracking

Per partition, `expected_seq` (0 = unknown). The first sequence seen seeds it; no gap is reported for the first packet.

- `seq == expected`: accept.
- `seq > expected`: emit `GAP_DETECTED {expected, received}`, count `gaps`, resync to `seq + 1`, and accept the data.
- `seq < expected` by at most `dup_window` (default 1024): duplicate, count `dups`, drop.
- `seq < expected` by more than `dup_window`: treated as a publisher restart without an admin message. Count `seq_resets`, emit `GAP_DETECTED` with `received < expected`, resync and accept.
- `SEQ_RESET` admin message: set `expected_seq = 0` so the next sequence is accepted as a fresh start. The admin record is published in-band.

**Heartbeat timeout.** A partition starts `INIT`, becomes `ACTIVE` on its first packet, and goes `DEGRADED` when no packet has arrived for `heartbeat_timeout_ms` (default 2000). Entering `DEGRADED` counts `degraded_events` and emits `TOPIC_DEGRADED` (reason `HEARTBEAT_TIMEOUT`). The next packet of any kind returns it to `ACTIVE`; the topic emits `TOPIC_RECOVERED` once none of its partitions is degraded. Any bus packet counts as liveness; a bus heartbeat message is just one.

## Ring and record format

Every record is two parts: a fixed header tuple that the agent fills from wherever the discriminators live (message root block today; packet header and message prefix fields, plus a per-partition extension tuple, are planned under roadmap item 1), then the payload bytes exactly as received. Apps never need a parent block to interpret a payload. Header-only shared definitions live in `shm/ring_layout.h` (app-facing ABI; changing it bumps `RING_LAYOUT_VERSION`). One file per session: `<fanout.ring_dir>/subplane.<agent>.<session>.ring`, created as `.tmp`, sized, mapped, filled, marked ACTIVE, then atomically renamed. Layout is `[RingHeader: 4096 B][data region: capacity B][reader slots: 64 × 512 B]`. The file mode is `agent.ring_mode` (default `0644`). Capacity is a power of two, at least 64 KiB; `max_payload` is at most `capacity / 4`. The whole file is reserved at creation (`posix_fallocate`), so a ring that does not fit in `/dev/shm` fails then, not later with SIGBUS, and pages are pre-faulted (`MAP_POPULATE`).

**Header page** (one cache line per independently written field group):

| Offset | Field | Notes |
|---|---|---|
| 0 | `magic` (u64), `layout_version` (u32 @8), `record_version` (u32 @12), `epoch` (u64 @16), `capacity` (u64 @24), `max_payload` (u32 @32), `writer_pid` (u32 @36), `session_name[32]` (@40) | Immutable after creation. Spills into line 1. `magic` is `"SPRING01"`. |
| 72 | `readers_offset` (u64), `reader_slots` (u32 @80), `reader_slot_bytes` (u32 @84) | Immutable. Where the reader slots are; `readers_offset` 0 means a ring without slots. |
| 128 | `head` (atomic u64) | Next write position, absolute bytes, never masked. |
| 192 | `heartbeat_ns` (atomic u64) | Writer liveness, CLOCK_REALTIME ns. |
| 256 | `state` (atomic u32) | 0 INIT, 1 ACTIVE, 2 CLOSED. |
| 320 | `records_written`, `bytes_written` (@328), `control_records` (@336) | Relaxed statistics. |
| 384 | `reader_flags` (atomic u32), `reader_sample_every` (atomic u32 @388) | Agent to every reader: `READER_FLAG_TRACE_LATENCY` (bit 0), and trace 1 record in N (0 or 1 = every record). |

**Reader slots.** The slot section follows the data region, so the data offset never moved and readers built before slots existed still work (they ignore the tail). Each app's `ShmReader` claims one 512-byte slot on `open()` (CAS on `owner_pid`), maps only the slot pages read-write, and releases the slot on `close()`. Every slot line has one writer:

| Offset | Written by | Contents |
|---|---|---|
| 0 | agent | `flags` (u32), `sample_every` (u32 @4): this reader only, OR'ed with the header's |
| 64 | app | `owner_pid` (u32; 0 free, `0xFFFFFFFF` being reclaimed), `identity_ready` (u32 @68), `start_time` (u64 @72, `/proc` field 22), `uid` (@80), `sdk_version` (@84), `capabilities` (@88; bit 0 traces latency), `name[32]` (@96, `SUBPLANE_APP_NAME`) |
| 128 | app | `read_pos` (u64), `heartbeat_ns` (@136), `overruns` (@144), `records` (@152), `active_flags` (u32 @160), about every 64 polls |
| 192 | app | read latency, `now - ingest_ts` of traced records: 38 power-of-two buckets (`[2^m, 2^(m+1))` ns, the last one open-ended), `lat_sum_ns` (@496), `lat_count` (@504) |

- **Cost to the reader:** one branch and one increment per `poll()`. Every 64th poll it reads the clock and writes its line. With tracing on, it reads the clock once per sampled record.
- **No slot is not an error:** a ring without slots, a full table, or no write permission on the file leaves reading unchanged; `slot_status()` says why. `ReaderOptions::report = false` opts out.
- **Reclaim:** the agent's control thread frees, once a second, a slot whose owner is both dead (no such pid, or the pid now has another start time) and silent for 30 s. Silence alone is a reader that stopped polling, and a missing pid alone may be a reader in another pid namespace. A reader whose slot was reclaimed sees another `owner_pid` and stops writing (`SlotStatus::Lost`).
- **Different uids:** claiming a slot opens the file read-write. A reader of another uid needs `agent.ring_mode: "0660"` (or `0664`) and a shared group. Read-only data is then enforced by the SDK's mappings, not by the file mode.
**Records.** A record is a 64-byte `TickHeader` plus payload, padded up to a multiple of 64 bytes (`record_bytes(len)`). Position in the data region is `pos & (capacity - 1)`. A record never straddles the end: if it would, the writer first emits a **PAD** record (`kind=PAD`, `msg_type=0xFE`) covering the remaining bytes, with `payload_len = skipped - 64`, then writes the record at offset 0.

**Publication.** Single writer, any number of read-only readers. Writer: plain-store header fields (except `seq_tag`) and payload; `seq_tag.store(pos, release)`; `head.store(pos + record_bytes, release)`. Reader: `h = head.load(acquire)`; `pos == h` is empty; otherwise check the window, then `seq_tag == pos` (else lapped), read, then re-check `head`. `publish` returns false (and the session counts `publish_failed`) if the payload exceeds `max_payload` or the ring is not ACTIVE; it never waits.

**Overrun.** The safe window is `capacity - record_bytes(max_payload)`, not `capacity`: the writer plain-stores the next record before advancing `head`, so a reader at `head - pos == capacity` may already be reading torn bytes. A reader seeing `head - pos > window` (or a `seq_tag` mismatch) reports `Overrun`, bumps its counter and resyncs to `head`. `RecordView::valid()` repeats the check after a zero-copy decode.

**Epoch and restart.** `epoch` is the agent start time in ns and is the same for every ring of one agent run. Every agent start creates new ring files and a new epoch. The old files are marked CLOSED and unlinked; existing mappings stay readable. The first record on every fresh ring is `AGENT_RESTARTED`. A reader detects restart by `state != ACTIVE`, a stale `heartbeat_ns`, or an epoch mismatch with a fresh `HELLO`.

### TickHeader

`core/message.h`. Exactly 64 bytes, `alignas(64)`, trivially copyable, `RECORD_VERSION = 1`. It is both the in-agent routing header and the on-ring record header.

| Offset | Field | Type | Meaning |
|---|---|---|---|
| 0 | `seq_tag` | u64 | On a ring: absolute byte position of this record, written last with release. 0 elsewhere. |
| 8 | `sequence` | u64 | Data records: the instrument sequence when the topic maps `instrument_sequence` (0 when the record carries none), otherwise the stream's sequence (one per partition). Other records: the stream's. 0 if the bus has none. |
| 16 | `exchange_ts` | u64 | ns since epoch from the mapped schema field. 0 if unmapped. |
| 24 | `ingest_ts` | u64 | ns since epoch at packet receive. Never 0 on a delivered record. |
| 32 | `symbol_id` | u32 | Dense interned id (or passthrough per config). |
| 36 | `payload_len` | u32 | Bytes after the header. |
| 40 | `template_id` | u16 | SBE templateId (0 for agent control). |
| 42 | `schema_id` | u16 | SBE schemaId. |
| 44 | `schema_version` | u16 | SBE version. |
| 46 | `venue_id` | u16 | Venue from the mapped field, 0 if unmapped. |
| 48 | `topic_id` | u16 | Topic that produced the record (registry id); 0 on agent-wide control records. |
| 50 | `msg_type` | u8 | `MsgType` for DATA, `ControlCode` otherwise. |
| 51 | `kind` | u8 | 0 DATA, 1 BUS_ADMIN, 2 AGENT_CONTROL, 3 PAD, 4 RAW (tap rings only: one received unit; see "Tap"). |
| 52 | `wire_format` | u8 | 30 = SBE. |
| 53 | `flags` | u8 | bit0 LVC-truncated, bit1 snapshot-origin. |
| 54 | `reserved[10]` | | Zero. |

Data `msg_type` values: 1 TRADE, 2 QUOTE, 3 BOOK_UPDATE, 4 BOOK_SNAPSHOT, 5 OHLCV, 6 STATUS, 7 DEFINITION (instrument definitions; their own type so the last-value cache does not mix them with status). There is no symbol string in the header; apps key by `symbol_id` and learn names from `SYMBOL_MAPPED` events or the control channel.

### Control codes

Payloads of AGENT_CONTROL records are packed little-endian PODs in `shm/control_events.h`. BUS_ADMIN records carry the original SBE bytes.

| Code | Name | Kind | Payload |
|---|---|---|---|
| 0x80 | BUS_HEARTBEAT | BUS_ADMIN | SBE bytes |
| 0x81 | BUS_SEQ_RESET | BUS_ADMIN | SBE bytes |
| 0x82 | BUS_SNAPSHOT_START | BUS_ADMIN | SBE bytes |
| 0x83 | BUS_SNAPSHOT_END | BUS_ADMIN | SBE bytes |
| 0x84 | BUS_TRADING_STATUS | BUS_ADMIN | SBE bytes |
| 0xA0 | GAP_DETECTED | AGENT_CONTROL | `GapEvent` {topic_id, expected_seq, received_seq, detected_ns}; `received < expected` means a publisher restart |
| 0xA1 | TOPIC_DEGRADED | AGENT_CONTROL | `TopicStateEvent` {u16 topic_id; u8 degraded; u8 partitions; u32 reason (`TopicReason`); u64 last_seq; u64 changed_ns}, 24 bytes. Sent each time the count of the topic's degraded partitions changes and is not zero. `partitions` is how many of the topic's partitions this agent reads. |
| 0xA2 | TOPIC_RECOVERED | AGENT_CONTROL | `TopicStateEvent`; sent when the count of degraded partitions reaches zero |
| 0xA3 | FILTER_RELOADED | AGENT_CONTROL | `FilterReloadedEvent` {session_id, generation} |
| 0xA4 | AGENT_RESTARTED | AGENT_CONTROL | `AgentRestartedEvent` {epoch, prev_epoch}; `prev_epoch` is currently always 0 |
| 0xA5 | SESSION_CLOSING | AGENT_CONTROL | `SessionClosingEvent` {session_id, reason: 0 shutdown, 1 destroyed or linger expired, 2 removed by reload} |
| 0xA6 | SYMBOL_MAPPED | AGENT_CONTROL | `SymbolMappedEvent` {symbol_id, venue_id, name_len, name[24]} |
| 0xA7 | TRANSPORT_DOWN | AGENT_CONTROL | `TransportStateEvent` {topic_id, reason, last_seq, changed_ns}, 24 bytes. Reasons are transport-neutral (INV-015): 1 source lost (the last Aeron image went away, a TCP peer closed, reset or refused), 2 local failure (Aeron driver or client timeout, socket error, a misframed TCP stream). Reported once per change: a transport already down reports nothing more. `last_seq` is the last sequence accepted before the change. |
| 0xA8 | TRANSPORT_UP | AGENT_CONTROL | `TransportStateEvent`; reason 0, or 3 publisher restart: a new Aeron session replaced the previous one. After a restart, on every partition, the next sequence is accepted as a fresh start, as after `BUS_SEQ_RESET`: the new publisher's numbering is not compared with the old one's, so it is neither reported as a gap or reset nor dropped as duplicates when it starts within `dup_window` below the old sequence. A restart is reported even without a DOWN before it (over Aeron UDP the old image outlives its publisher by the image liveness timeout). It is written ahead of the records of the batch that brought the source back; on Aeron, when a poll sees a new image before the client has announced it, a plain UP comes first and the restart UP follows on the next poll, possibly after a few records. Over UDP multicast "up" is the first datagram, or the first after a socket error; a silent sender is `TOPIC_DEGRADED`, not a transport state. Both events go to every session of the partition that takes `agent_control`. `/partitions` shows the state (`unknown`, `up`, `down`) and the counts. |
| 0xA9 | SYMBOL_STALE | AGENT_CONTROL | `SymbolStaleEvent` {symbol_id u32, topic_id u16, reason u16, last_seq u64, changed_ns u64}, 24 bytes. The session's view of the symbol is no longer exact and a recap of it has started. `reason`: 1 sequence gap, 3 publisher restart, 4 sequence reset (`BUS_SEQ_RESET` or a backward jump beyond `dup_window`), 5 instrument gap (this symbol only); 2 (transport down) is reserved and never sent. `last_seq` is the partition's last sequence before the change. Written only to recap sessions, once per symbol of theirs on the partition. |
| 0xAA | RECAP_PENDING | AGENT_CONTROL | `RecapEvent` {request_id u64, symbol_id u32, topic_id u16, status u8, reserved u8, attempt u32, record_count u32, as_of_seq u64}, 32 bytes. The symbol is waiting for a snapshot; `request_id` and `attempt` are 0. |
| 0xAB | RECAP_BEGIN | AGENT_CONTROL | `RecapEvent`; `as_of_seq` is S, `record_count` the records in the reply, `attempt` 1 for the first request and +1 per retry |
| 0xAC | RECAP_COMPLETE | AGENT_CONTROL | `RecapEvent`; `as_of_seq` is S, `record_count` the snapshot records written to this ring |
| 0xAD | RECAP_FAILED | AGENT_CONTROL | `RecapEvent`; `status`: 1 timeout, 2 rejected, 3 snapshot too old, 4 hold overflow, 5 reserved (a gap while a symbol is pending is reported as `SYMBOL_STALE`), 6 publisher identity mismatch (planned), 7 request could not be sent, 8 reply larger than `reply_slab_bytes`. The agent asks again; the symbol stays pending. |
| 0xAE | INSTRUMENT_GAP | AGENT_CONTROL | `InstrumentGapEvent` {symbol_id u32, topic_id u16, reserved u16, expected_seq u64, received_seq u64, detected_ns u64}, 32 bytes. One instrument's sequence jumped (see "Topics and partitions", instrument sequence); written before the record that jumped; `received > expected` always. |
| 0xFE | PAD | PAD | none |

Every event's topic field is `topic_id` (it was `partition_id`, same layout). Bus admin delivery is topic-scoped (the session must accept the record's topic). Agent control events honour the session's `agent_control` flag. `GAP_DETECTED`, `TOPIC_DEGRADED` and `TOPIC_RECOVERED` go to the sessions whose patterns take that topic; `INSTRUMENT_GAP` only to the sessions that take the record. `SYMBOL_MAPPED` goes to every session in the thread group. `AGENT_RESTARTED`, `FILTER_RELOADED` and `SESSION_CLOSING` are written only to the ring of the session they concern; the four recap events only to the rings of the recap sessions waiting on that symbol, and `SYMBOL_STALE` only to recap sessions.

## Filters

`core/filter_table.h`. A `FilterTable` is a symbol bitset sized from `agent.max_symbols`, a `msg_type_mask`, a `topic_mask` (a 32768-bit bitmap over `topic_id`; an empty pattern list sets all of it), a `venue_mask` (default all), flags `all_symbols`, `bus_admin`, `agent_control`, and a `generation`. `accept` is a handful of AND and shift operations with range guards. It is built on the cold path and swapped by pointer.

The venue mask is part of the table and the hot path, but neither YAML nor the control protocol can currently set it, so it is always "all venues".

## Sessions

A `Session` is a filter table plus a ring plus counters.

- **YAML sessions** (`sessions:`) have `owner=config`. They survive client disconnects. `POST /reload` re-reads the file and reconciles by name: new names are created, changed filters are swapped, names that disappeared are destroyed (reason 2). A session's topics cannot change after creation; destroy and recreate. Only the `sessions` section is reloaded; a change to `registry`, `topics` or `agent.thread_groups` is reported in the reply (`"ignored"`) and logged as `RELOAD_IGNORED`, and needs a restart. There is no SIGHUP handler; reload is via HTTP.
- **Client sessions** (`owner=client`) are created over the control channel. They live while any connection that created or attached to them is open, plus `control.linger_ms` (default 5000) after the last holder disconnects; a new `ATTACH` before expiry keeps them. Reload never touches them. A name collision is rejected (`EXISTS`), and a YAML entry that collides with a client session is ignored with an error.
- Session names are 1 to 31 characters of `[A-Za-z0-9_-]`. Symbols in a filter are names, decimal integer ids, or `<venue>:<id>` on `symbol_scope: venue` partitions (a bare id is rejected when every partition of the session is venue-scoped, and `<venue>:<id>` when none is), and are interned on the cold path. An empty `topics` list means all topics; a pattern that matches no topic is an error. An empty `msg_types` list means all data types.

## Control channel

`control/control_proto.h`, served by `ControlServer` on its own thread. `AF_UNIX SOCK_SEQPACKET`, so every request and reply is exactly one datagram. Default path `/run/subplane/<agent>.sock` (`control.socket_path`); if that directory is missing or not writable, it falls back to `control.fallback_socket_path`. Peer credentials are logged. Up to 64 clients.

Every datagram is `CtlHeader {u32 len, u16 op, u16 version, u32 req_id, u32 status}` (16 bytes, little-endian, packed) followed by `len` body bytes. A reply echoes `op` and `req_id`. `status != 0` means the body is a UTF-8 error message. Datagrams over 64 KiB are rejected.

| Op | Code | Request | Reply |
|---|---|---|---|
| HELLO | 1 | empty | `CtlHelloResp`: agent name, epoch, record_version, proto_version, symbol count, max symbols, schema id and version of the first partition, `topic_count`; then `topic_count` × `CtlTopicEntry` {u16 topic_id; u16 partitions; char name[48]}, 52 bytes each |
| ATTACH | 2 | session name | session id, owner, ring path, epoch, capacity, max_payload, filter generation |
| CREATE_SESSION | 3 | `CtlSessionReq` (68 bytes: name, ring capacity, max_payload, msg_type_mask (0 = all), `topic_count`, two reserved fields, the `all_symbols`, `bus_admin`, `agent_control` and `recap` flag bytes, `symbol_count`; it replaces the u64 `partition_mask`, so the size and `CONTROL_PROTO_VERSION` are unchanged), then `symbol_count` × `CtlSymbolName` (24 B each), then `topic_count` × `CtlTopicPattern` {char pattern[48]}; no patterns = all topics | same as ATTACH |
| SET_FILTER | 4 | same body (ring fields ignored) | new filter generation |
| DESTROY_SESSION | 5 | session name | empty |
| SNAPSHOT | 6 | session name | multi-datagram, see below |
| PING | 7 | client ns | client ns, server ns |
| SYMBOLS | 8 | from_id, max | paged id to name entries (up to 1024 per reply) |
| IDENTIFY | 9 | app name (char[32], 1 to 31 printable chars, no quote or backslash) | empty. Optional: names the connection's app in `/topology`. An older agent answers `UNSUPPORTED`, which the SDK treats as success |

Statuses: `OK`, `BAD_REQUEST`, `NOT_FOUND`, `EXISTS`, `INTERNAL`, `UNSUPPORTED`, `TOO_LARGE`. A client ring may be at most 1 GiB.

**Snapshot framing.** Reply datagrams all carry op SNAPSHOT and the request's `req_id`: (1) a header body `{ring_head_at_snapshot, record_count, skipped}`; (2) `record_count` datagrams, each a 64-byte `TickHeader` plus payload, with the snapshot-origin flag set; (3) an 8-byte end body `{record_count, done=1}`. Records too large for a datagram are skipped and counted. The session's current filter selects what is sent.

## Last-value cache

`pipeline/lvc.h`. One slot per (symbol id, tracked data type), where tracked types are data types 1 to 7. Each slot is a seqlock: a version counter, the `TickHeader`, and up to `lvc.max_payload` bytes (default 512). Larger payloads store the header only with the truncated flag. The single writer is the pipeline thread; cold readers (control channel snapshot, HTTP) retry on an odd or changed version. Storage is `max_symbols * 7` slots allocated at startup. A partition with `lvc: off` writes nothing to the cache, so snapshots and `/lvc` hold no values for its symbols.

Export of the cache into shared memory for direct app reads is planned.

**Scope.** The cache is for self-contained values only: the newest message for a key is the whole state for that key (instrument definition, trading status, last quote, last trade). Books and accumulated values (volume, VWAP) are built from many messages with the publisher's rules; they are never cached or computed here and come from recap. A per-partition switch to turn the cache off where a snapshot service exists is planned.

## Recap (partly implemented)

Design: `docs/context/project-vision.md`, "Recovery: recap". Contract summary:

**Declaring a snapshot service** (implemented). A partition gains an optional `recap:` block; the recap component is compiled in and selected by `type`, like a transport (`recap_components()` in `src/registry/builtin_components.cpp`):
```yaml
recap:
  type: aeron                       # v1; tcp for buses that are not Aeron later
  aeron_dir: ""                     # media driver directory; empty = Aeron default
  request:  { channel: "aeron:udp?endpoint=snap-host:40500", stream_id: 1101 }
  response: { channel: "aeron:udp?control-mode=response|control=snap-host:40501", stream_id: 1102 }
  sections: { channel: "aeron:udp?endpoint=239.1.2.3:40502|interface=10.0.0.0/8", stream_id: 1103 }   # the service's shared section stream; omit for a 1:1 service
  reply: spp                        # spp (default) | stream_markers; see "Reply contract"
  timeout_ms: 2000                  # per request; a bulk's is silence on the section stream
  max_inflight: 64                  # outstanding requests per partition; more wait in FIFO order
  coalesce_ms: 20                   # a symbol's first request waits this long for others; 0 sends at once
  retry: { base_ms: 50, max_ms: 5000 }   # exponential, jittered within [cap/2, cap]
  reply_slabs: 8                    # preallocated reply buffers per partition
  reply_slab_bytes: 1048576         # the largest reply the agent accepts
  max_pending: 4096                 # symbols pending at once on this partition
  hold_bytes: 4194304               # preallocated arena for the live messages held for pending symbols
  hold_chunk_bytes: 256             # its allocation unit
```
A partition can recap only if its symbol is an integer (the request names the instrument as a u64) and it has a sequence (`framing.sequence`; the merge places a snapshot against live messages by sequence). With `reply: stream_markers` its decoder must also map `RECAP_BEGIN` and `RECAP_END`, which rules out `dbn`. Otherwise startup fails with the reason.

**The `aeron` component.** It adds a subscription on the response channel (an Aeron response channel, `control-mode=response` with `control=` the service's response endpoint), then a request publication carrying `response-correlation-id=<that subscription's registration id>`. The service answers on a response publication made with the requesting image's correlation id, so each agent receives only its own replies. The agent's request channel must not set `response-correlation-id` itself. Requests are small (a `RecapRequest` frame is 60 bytes), so `|term-length=65536` on the request channel (and on the service's response publication, sized to its largest reply) keeps `/dev/shm` use small; Aeron's 16 MB default term length costs about 48 MB per publication, which a container's 64 MB `/dev/shm` cannot hold twice. With `sections:` it also subscribes to the service's section stream (multicast, multi-destination or IPC), which every agent on the partition reads; the service's publication sets the flow control (`fc=max`, the multicast default, keeps a slow agent from holding the others back: it loses a section and asks again).

**The recap manager** (`recap/recap_manager.h`): one cold thread per agent, started after the control server when any partition has a recap block. Each pipeline group talks to it through three SPSC queues: requests (`REQUEST` or `CANCEL` for a symbol, produced only by the pipeline, which owns who waits), replies, and slab releases. Per partition it:
- coalesces: one upstream request per symbol however many sessions ask (`recap_coalesced`). A symbol's first request waits `coalesce_ms` (default 20, 0..10000) before it goes out, so sessions that ask together share it: a reload or apps restarting with the agent create sessions a few milliseconds apart, and a recap on a fast link completes in less, so without the window each would send its own (measured: 16 sessions on 2 instruments sent 32 requests; 20 ms made it 6, 200 ms made it 2). A retry queued behind a waiting request waits at most `coalesce_ms` longer; a renew does not wait;
- renews: every request and reply carries the pipeline's recap generation for the partition, which rises when the stream changes under the pending recaps (a gap, a sequence reset, a publisher restart). A `REQUEST` with a newer generation than the symbol's request abandons the one in flight (its reply, then or later, is stray; one half assembled is skipped to its end), resets the attempt count and sends a fresh request at once, ahead of the queue (`recap_renewed`). There is no separate renew op;
- sends SPP `Hello` (`Role` agent, `PeerEpoch` = the agent's epoch, versions 1 to its own) as the first message whenever the component reports a connection, again after every reconnect, and keeps the service's `Hello` per connection. A service whose `MinProtoVersion` is above the agent's version gets `Status{Unsupported}` and no requests until it reconnects; those requests fail as `SEND_FAILED` and are retried;
- numbers requests per agent epoch (the `CorrelationId`; `AgentEpoch` = epoch) and fills `Instrument`/`Venue` from the symbol table's reverse lookup (or the raw id in passthrough mode) and `LastSeqSeen` from the partition;
- limits outstanding requests to `max_inflight`, the rest wait in FIFO order;
- on a timeout, a rejection, an overflow or a failed send, reports the failure to the pipeline and retries after the backoff, for as long as a session waits (the pipeline sends `CANCEL` when the last waiter leaves; there is no attempt limit);
- assembles a reply into a free slab: the begin (a known request id, with S) opens it, each record is stored as a u32 little-endian length plus the message bytes, the end closes it and the slab goes to the pipeline until it is released. A reply larger than `reply_slab_bytes` is an overflow (logged, at most every 10 s); with no free slab the reply is skipped and times out. The response channel and the section stream each have their own assembly, so a reply and a section can arrive at once;
- takes **sections**: a reply whose `RecapBegin` names its `Instrument` is matched by instrument, not by request id, so a section the service published for any agent serves every agent waiting on that instrument, asked for or not (`recap_sections`). It is skipped, and not counted stray, when nobody here waits on the instrument or its S is below the symbol's floor (`recap_sections_skipped`; the floor comes with each `REQUEST`). A section for a symbol whose own request has not gone out yet completes it, and the request is never sent;
- sends **bulk** requests: a `REQUEST` marked bulk (posted for a partition-wide resync, below) joins one `RecapRequest{Flags: Bulk}` for the whole partition (`recap_bulk_requests`), answered by sections. Its deadline is silence: every section on the stream pushes it back by `timeout_ms`, so a long, paced burst never expires while it arrives. Symbols still waiting when it goes quiet fail once (`TIMEOUT`) and are asked for on their own; a symbol added while a bulk is out goes on its own too. A single request for a symbol on the bulk joins it (`recap_coalesced`).

**Reply contract for a snapshot service.** A reply is a begin, the records, an end, sent contiguously on its stream (replies to different requests are not interleaved; a reply may span offers); a reject replaces a reply. A service with a section stream answers requests with sections there, published once per instrument for every agent that asked (subplane-feeds' `spwsgw-snap` does, coalescing and pacing them); one without answers each request on its response channel. Messages outside a reply, unknown request ids and late replies are counted (`recap_stray`) and dropped. Two modes, set by `recap.reply`:
- **`spp`** (default): the SPP messages `RecapBegin` (`AsOf` = S, `SourceEpoch`, and on a section `Instrument` and `Venue`), `RecapRecord` (`Payload`: one stream message, the publisher's bytes, any encoding), `RecapEnd` (`RecordCount`), `RecapReject` (`Reason`), each with the request's `CorrelationId` (a section's: its section id) and its u32 length prefix (`docs/protocol/subplane-protocol.md`). A `RecapEnd` whose `RecordCount` differs from the records received drops the reply (`recap_stray`), and the request times out and is retried. A non-Ok `Status` answering a request counts as a reject (`RecapReply.reason` = 0x100 + the code). Unknown or unexpected SPP messages are counted and, when they carry a `CorrelationId`, answered `Status{Unsupported}`; must-understand `Extension`s likewise, others are ignored. This mode works for every decoder, `dbn` included, because the agent never parses the records until it merges them.
- **`stream_markers`**: for a firm bus whose snapshot service already answers in the bus's own schema. The reply is messages of the stream's schema, framed like the live stream, between decoder-mapped markers (below). The request is SPP in both modes.

**Counters** (per recap partition, in `/metrics` under `recap` and in `/partitions` as `recap`): from the manager `recap_requests`, `recap_coalesced`, `recap_replies`, `recap_rejects`, `recap_timeouts`, `recap_overflows`, `recap_send_failed`, `recap_stray`, `recap_cancelled`, `recap_no_slab`, `recap_renewed`, `recap_bulk_requests`, `recap_sections` (sections taken), `recap_sections_skipped` (sections for symbols nobody here waits on, or older than the floor), and the gauges `recap_inflight`, `recap_queued`; from the pipeline `recap_held` (live messages held), `recap_merged`, `recap_records` (snapshot records delivered), `recap_too_old`, `recap_hold_overflows`, `recap_post_dropped` (a request or release that found both the queue and the pipeline's preallocated backlog full; should stay 0), `recap_resyncs` (gaps, resets and restarts seen on a recap partition), `recap_superseded` (replies dropped because a newer generation renewed their request), `recap_elsewhere` (an `UnknownInstrument` from this partition's service while another partition of the topic still tries; see "Topics with several partitions").

**Reply markers, in decoder config** (implemented; used by `reply: stream_markers`). The reply is ordinary messages of the stream's own schema between two markers, mapped like any admin message, with the fields the merge needs:
```yaml
messages:
  - { template: 21, admin: RECAP_BEGIN,  fields: { request_id: RequestId, as_of: AsOfSeq, source_epoch: SourceEpoch } }
  - { template: 22, admin: RECAP_END,    fields: { request_id: RequestId } }
  - { template: 23, admin: RECAP_REJECT, fields: { request_id: RequestId, reason: Reason } }
```
`request_id` is required on every marker and `as_of` (the sequence S the snapshot is consistent with) on `RECAP_BEGIN`; `source_epoch` and `reason` are optional. For `sbe_routing` each is a root-block integer field of that template; for `flatbuffers` each is `{path, type}` with an integer type, like a routing field. A marker that arrives on the live stream is counted in `recap_markers` and dropped: apps never see a publisher's markers, only the agent's own recap events. `dbn` has no marker records: Databento's record types are fixed, so a DBN partition recaps with `reply: spp`.

Records between the markers are delivered with the snapshot-origin flag and `TickHeader::sequence` set to S; their payload is untouched. Only data records of the requested symbol are delivered, each through the waiting session's own filter; other records are skipped. Snapshot records do not update the last-value cache.

**Upstream request** (implemented). The SPP message `RecapRequest` (template 10 of `schemas/subplane_proto.xml`, encoded with `include/subplane/proto/subplane_proto_codec.h`): `CorrelationId` (unique per agent epoch, echoed in the reply), `AgentEpoch`, `Instrument` (the symbol as on the wire, not the agent's `symbol_id`), `LastSeqSeen` (0 if none), `SourceEpochSeen` (0 if unknown; always 0 until publisher identity lands), `Venue` (as on the wire, 0 if unmapped), `Flags` (bit 0 Bulk: every instrument of the partition, `Instrument` 0). One frame per request, after the connection's `Hello`. It replaced the earlier 64-byte `SPRQ` request before any release.

**Publisher identity** (planned). On Aeron, a new image session id means a publisher restart. Elsewhere the stream carries a publisher start time (`SourceEpoch`), mapped in decoder config. A reply whose identity differs from the live stream is discarded and requested again.

**Sessions** (implemented). YAML `recap: true` on a session, or the control channel `CtlSessionReq.recap` byte (was padding; old clients send 0). It is fixed at creation: `SET_FILTER` keeps it, and a config reload that changes it is refused (destroy and recreate). A recap session needs a symbol list (not `all_symbols`, in v1) and `agent_control`. The distinct symbols of all recap sessions on a recap partition must fit in its `max_pending`; a session that would exceed it is refused. On a partition without a `recap:` block the flag does nothing.

**Triggers** (implemented). A recap session starts, or adds symbols with `SET_FILTER`. And the stream changing under it: a sequence gap, a sequence reset (`BUS_SEQ_RESET`, or a backward jump beyond `dup_window`) or a publisher restart (`TRANSPORT_UP` reason 3). Each of these makes every symbol of every recap session on the partition stale and pending (partition-wide, because the sequence is per stream, not per symbol; on a topic with several partitions, only the symbols last delivered on this one or not yet seen): the session gets `SYMBOL_STALE{reason}` for each of its symbols, then `RECAP_PENDING` for each one that was live. The partition's recap generation rises, so every pending request is renewed and any reply to an older one is dropped. These requests are marked bulk: the manager asks the service for the whole partition in one request. A transport going down and coming back is not a trigger by itself: what was missed shows as a gap, and a new publisher as a restart. Sessions without the flag get only the usual `GAP_DETECTED` or `TRANSPORT_UP`.

**Merge**, per (partition, symbol), on the pipeline thread with preallocated memory only (INV-002; `pipeline/recap_state.h`). Implemented:
1. **Pending.** The first recap session to wait on a symbol creates its pending entry with a **floor**, the partition's last sequence at that moment, and the pipeline posts one `REQUEST`; later waiters join the same entry. Each waiting session gets `RECAP_PENDING` and a waiter bit. When the last waiter leaves (filter change, session removed) the entry and its holds are freed and `CANCEL` is posted.
2. **Hold.** For a pending symbol, fan-out skips the waiting sessions and appends the message once to the symbol's hold list (header and payload, in a chunked arena of `hold_bytes`); every other session gets it at once. With nothing pending the hot path costs one load and a branch. The sequence check and the last-value cache run before fan-out as always.
3. **Reply.** A snapshot as of S merges when `floor <= S <= last sequence seen`: to each waiter, `RECAP_BEGIN`, the reply records, `RECAP_COMPLETE{S}`, then the held messages with sequence above S, each through that session's filter. The entry is freed and the slab released. A reply with S ahead of live is kept until live reaches S. A reply with S below the floor is `RECAP_FAILED{3 too old}` and requested again: the messages between S and the floor are in neither the snapshot nor the hold.
4. **Failures raise the floor.** A hold overflow frees that symbol's holds and raises its floor to the message that did not fit (`RECAP_FAILED{4}`, once until a hold succeeds again); an older reply then fails as too old and is requested again. A gap, a reset or a restart frees every hold and every kept reply of the partition, since they describe the stream as it was, and renews the requests (Triggers). After a gap the floors move to the sequence before the one received. After a reset or a restart they wait for the new stream's first sequence and become the one before it; until then an OK reply is kept, not compared.
5. Manager failures (timeout, rejection, a reply too large, a failed send) reach the waiters as `RECAP_FAILED` with the attempt number; the manager retries on its own for as long as anyone waits.
Planned: the publisher identity check (status 6, with canonical v2's `SourceEpoch`).

**Topics with several partitions** (implemented). Each partition has its own snapshot service, the one that follows its stream, so S is that stream's sequence and the merge is unchanged. The pipeline keeps, per topic, the partition each symbol was last delivered on (`Partition::inst_home`, one byte per symbol, only on topics with two or more recap partitions in the group). A recap session's symbol is asked of that partition only; one not seen yet is asked of every partition. A service that does not have it answers `RecapReject{UnknownInstrument}`: that partition waits quietly (`recap_elsewhere`) while another still tries, and fails (`RECAP_FAILED`) only when every partition has said so. When one partition merges the symbol, the others drop those sessions' waits and cancel. A session gets one `RECAP_PENDING` per symbol however many partitions are asked.

**What a recapping session sees for symbol X:** `SYMBOL_STALE` (when a gap, reset or restart triggered it), `RECAP_PENDING` (unless it was pending already), then `RECAP_BEGIN`, the snapshot records, `RECAP_COMPLETE{S}`, then live data in sequence order with no gap and no duplicate. Each failure on the way is a `RECAP_FAILED`. Sessions without the flag see none of this, and nothing they receive is delayed.

**Canonical schema.** Version 2 (planned) adds `RecapBegin` (21), `RecapEnd` (22), `RecapReject` (23) and `SourceEpoch` on `Heartbeat`, so the feeds package's `spwsgw-snap` can serve canonical streams. A firm's own bus maps its own markers instead.

## Tap

A partition with `tap:` copies every unit its transport delivers into a **tap ring**, for a recorder (`docs/context/recording-format.md`). Built: the tap ring and its context file. Planned: `subplane record`, which writes them to `.pcapng` files.

- **Where:** `<fanout.ring_dir>/subplane.<agent>.tap.<topic>.<n>.ring`, an ordinary ring (same layout, reader slots, heartbeat, `CLOSED` on shutdown) whose records are:
  - `Kind::RAW` records: one per received unit, before framing, so malformed, duplicate and unknown units are kept too. The payload is the unit's bytes (a UDP payload, a TCP stream record, an Aeron message), `ingest_ts` is its receive time, and `sequence` counts every unit offered to the tap from 1: a gap means units the recording lacks.
  - `TRANSPORT_UP`/`TRANSPORT_DOWN` control records, as sessions get them.
- **The context file:** `subplane.<agent>.tap.<topic>.<n>.ctx`, beside the ring and written before it (atomically), holds the CONTEXT TLVs of the recording format: agent name, version and epoch, the partition's YAML as loaded, the schema XML (`sbe_routing`), their SHA-256, the timestamp source and the synthetic UDP destination. A recorder checks its epoch against the ring's.
- **Cost:** off, one predictable branch per packet. On, one copy of each unit into the ring, which never blocks: a slow recorder is lapped, and a unit larger than `max_payload` is counted in `tap_dropped`. Measured (`subplane replay --bench --tap`, 3 messages per packet, pinned): about 30 to 40 ns per message more with the tap on.
- `/partitions` shows the tap's ring, counts and the slowest reader's lag; `/topology` lists the ring downstream with role `record`.

## Relay (planned)

A separate process (working name `sprelayd`), not the agent: the agent never publishes data (INV-017). It tunnels the bus (gateway) protocol between sites, never an exchange protocol.
- **Tunnel layer:** Aeron fragments in, the same bytes out; output is TCP, Aeron UDP or multicast, so one config direction is Aeron to TCP and the other TCP to Aeron. Each fragment travels as one length-prefixed TCP frame. Sequence numbers and publisher identity are untouched, so recap merges stay valid at every hop. Heartbeats on idle links; a bounded buffer per connection, and a downstream that falls behind is disconnected and recovers by recap.
- **Recap-proxy layer** (optional per relay): to the sites below it the relay is the snapshot service; it coalesces their requests and asks upstream once per symbol. Later, downstream interest (symbols, partitions) filters what crosses the link.

## Client SDK

Two header-only files, libc and the standard library only. They re-export `shm/ring_layout.h`, `shm/control_events.h` and `core/message.h`.

- `client/shm_reader.h`: `ShmReader` maps a ring's header and data `PROT_READ`, claims a reader slot (see "Reader slots") and validates magic, layout version, record version, capacity and size (otherwise `LayoutMismatch`). `poll(RecordView&)` returns `Ok`, `Empty`, `Overrun` (position resynced to head), or `Closed`; `liveness()` and `writer_alive()` report `WriterGone` on a stale heartbeat. Also `seek`, `seek_to_head`, `head`, `lag`, `overruns`, `epoch`, and for the slot `slot_status`, `tracing`, `report_now` (`ReaderOptions::report = false` opts out). `poll` never blocks, throws or allocates. PAD records are skipped transparently.
- `client/control_client.h`: `ControlClient` with blocking calls and a receive timeout (default 5 s), not thread-safe: `connect`, `hello`, `attach`, `create_session`, `set_filter`, `destroy_session`, `snapshot`, `symbols`, `ping`, `identify`. When `SUBPLANE_APP_NAME` is set, `connect` sends `IDENTIFY` with it (best effort).

**Late-join idiom** (also `examples/shm_demo_app.cpp`):

1. Create or attach the session, open the ring, check `rd.epoch()` against `hello().epoch`.
2. `mark = rd.head()`.
3. `snapshot(name, ...)` and apply every record to local state, remembering the last `sequence` per symbol.
4. `rd.seek(mark)` and tail. Skip data records whose `sequence` is not newer than what the snapshot had (the overlap).
5. After decoding a zero-copy view, call `valid()`; if false, drop what was decoded and resnapshot. On `Overrun`, resnapshot.
6. On `Closed` or `WriterGone`, reattach and resnapshot.

Apps decode the SBE payload with their own generated codecs. A Python client is planned.

**Typed views for flat messages.** An SBE message with no groups and no varData is a fixed block behind the 8-byte message header, so `subplane schema --views` emits typed access to it in three forms from the same XML the agent loads (`decoder/sbe_views.h`): a C++ header, a NumPy module and a q script. No codec, no code generation step beyond the one tool run, and never a `#pragma pack`: a message whose fields are all naturally aligned at their payload offsets becomes a plain struct whose `static_assert`s prove the layout, and one with any misaligned field (SBE does not pad; the sample `Trade` has an `int64` at offset 34) becomes a view class with `memcpy` accessors, correct on every platform and the same loads on x86. `view<T>(payload, len)` checks the length, template id and schema id first and returns a pointer (struct form) or an optional (view form). From the sample schema:

```cpp
// Ticker: template 4, 32 bytes.
struct Ticker {
    static constexpr bool kIsStruct = true;
    static constexpr uint16_t kTemplateId = 4;
    static constexpr size_t kSize = 32;
    uint16_t blockLength;
    uint16_t templateId;
    uint16_t schemaId;
    uint16_t version;
    char Symbol[8];  // Symbol8
    uint64_t SeqNum;
    uint64_t TransactTime;
};
static_assert(sizeof(Ticker) == Ticker::kSize);
static_assert(offsetof(Ticker, blockLength) == 0);
static_assert(offsetof(Ticker, templateId) == 2);
static_assert(offsetof(Ticker, schemaId) == 4);
static_assert(offsetof(Ticker, version) == 6);
static_assert(offsetof(Ticker, Symbol) == 8);
static_assert(offsetof(Ticker, SeqNum) == 16);
static_assert(offsetof(Ticker, TransactTime) == 24);

// Trade: template 1, 47 bytes. View class, not a struct: Price at offset 34 is not naturally aligned.
class Trade {
public:
    static constexpr bool kIsStruct = false;
    static constexpr uint16_t kTemplateId = 1;
    static constexpr size_t kSize = 47;
    explicit Trade(const uint8_t* p) noexcept : p_(p) {}
    const uint8_t* data() const noexcept { return p_; }
    uint16_t blockLength() const noexcept { return detail::load<uint16_t>(p_, 0); }
    uint16_t templateId() const noexcept { return detail::load<uint16_t>(p_, 2); }
    uint16_t schemaId() const noexcept { return detail::load<uint16_t>(p_, 4); }
    uint16_t version() const noexcept { return detail::load<uint16_t>(p_, 6); }
    uint64_t SecurityID() const noexcept { return detail::load<uint64_t>(p_, 8); }
    uint64_t SeqNum() const noexcept { return detail::load<uint64_t>(p_, 16); }
    uint64_t TransactTime() const noexcept { return detail::load<uint64_t>(p_, 24); }
    uint16_t MarketSegmentID() const noexcept { return detail::load<uint16_t>(p_, 32); }
    int64_t Price() const noexcept { return detail::load<int64_t>(p_, 34); }
    uint32_t Qty() const noexcept { return detail::load<uint32_t>(p_, 42); }  // Qty32
    uint8_t Side() const noexcept { return detail::load<uint8_t>(p_, 46); }  // SideEnum
};
```

The Python module defines one frozen dataclass per message (standard library `struct`, no dependencies) with `from_bytes`, a `RecordHeader` dataclass for the 64-byte `TickHeader`, and `parse(record)` that turns one ring record into `(RecordHeader, message)` by template id, so a ring or a recording can be read from Python without the C++ SDK. When NumPy is importable each class also carries a structured `DTYPE` with explicit offsets for columnar, zero-copy reads (`np.frombuffer(payloads, dtype=Trade.DTYPE)`). The q script emits a `1:` fixed-width spec and a `parse` function per message (unsigned fields widen to the signed q type of the same width; char arrays are cut from the raw bytes); it is generated and checked structurally but has not been run under q here. Little-endian schemas only; messages with groups or varData are reported as skipped, never partially emitted; constant fields are omitted and listed in a comment; enums and sets appear as their encoding primitive with the type name noted. `tests/data/sample_md.xml` is run through the tool at every build and the generated header is compiled and exercised by `test_sbe_views`; `ctest -R schema_views_numpy` loads the NumPy module (skipped where NumPy is absent). Scalar arrays (`<type primitiveType="int64" length="10">`) are typed in every view: `int64_t BidPx[10]` in a struct, an indexed accessor in a view class, a `('<i8', (10,))` dtype field, a tuple in the dataclass, a list in q.

**Canonical schema.** `schemas/subplane_canonical.xml` (package `subplane.canonical`, schema id 100, version 1) is the normalized tick shape that the transcode mapping (planned, encoding-agnostic step 4), the kdb+ feed, the recorder index, the Python client and the feeds package target. Every message is flat and every scalar is naturally aligned at its payload offset, so all eight are struct views; `schemas/CMakeLists.txt` generates `subplane_canonical_views.{h,py,q}` into `build/generated/` at every build and `test_canonical` compiles the header and reads encoded messages through it. Rules: prices and quantities are `int64` scaled by 10^-9 (declared as the constant `PriceExponent = -9`; 1.0 is 1,000,000,000; range ±9.2e9), no floats, no per-message exponent; every data message starts with the same 40-byte prefix (`InstrumentId u32, VenueId u16, Flags u16, Sequence u64, InstrumentSeq u64, ExchangeTs u64, RecvTs u64`, payload offsets 8..48; `Sequence` counts the stream, `InstrumentSeq` the instrument across the topic, 0 = not counted), so routing fields sit at fixed offsets in every message; symbols are ids and names arrive as `InstrumentDefinition` messages (which is how a gateway assigns ids); the book snapshot is ten levels per side in fixed arrays, deeper books are raw passthrough; `BookUpdate` is one level per message.

| Template | Message | Fields after the prefix | Payload bytes |
|---|---|---|---|
| 1 | `Trade` | `Price`, `Qty`, `TradeId u64`, `Aggressor` (Side) | 80 |
| 2 | `Quote` | `BidPx`, `BidQty`, `AskPx`, `AskQty` | 80 |
| 3 | `BookUpdate` | `Price`, `Qty` (0 on delete), `Side`, `Action` (Add/Update/Delete/Clear), `Level u8` | 72 |
| 4 | `BookSnapshot` | `BidPx[10]`, `BidQty[10]`, `AskPx[10]`, `AskQty[10]`, `Depth u8` | 376 |
| 5 | `Ohlcv` | `Open`, `High`, `Low`, `Close`, `Volume`, `IntervalSec u32` | 96 |
| 6 | `Status` | `TradingStatus`, `Reason u8` | 56 |
| 10 | `InstrumentDefinition` | `Symbol char[32]`, `Exchange`, `Base`, `Quote` (`char[16]`), `TickSize`, `LotSize`, `Status` | 152 |
| 100 | `Heartbeat` | prefix only, `InstrumentId` 0; `StreamId i32` at offset 40 (0 = unstamped) | 56 |

A partition carrying it is ordinary `sbe_routing` configuration: `fields: {symbol: InstrumentId, sequence: Sequence, instrument_sequence: InstrumentSeq, exchange_ts: ExchangeTs, venue: VenueId}` (plus `stream_id: StreamId` to check the stream identity) and templates 1..6 mapped to the matching `MsgType`, 10 to `DEFINITION`, 100 to `admin: HEARTBEAT` (the commented `canonical.sample` topic in `config.yaml`). `subplane send --schema schemas/subplane_canonical.xml` publishes synthetic canonical messages (it binds field names from either the sample or the canonical schema), and `examples/views_app.cpp` consumes a ring through the generated views (`--spin` busy-polls; `--latency` prints histograms of canonical `RecvTs` to app read, ring `ingest_ts` to app read, and `RecvTs` to `ingest_ts`, all CLOCK_REALTIME on one box, so a gateway, the agent and an app measure end to end with no probes); `ctest -R canonical_views_app` runs sender, agent and app end to end. The generated `Trade`:

```cpp
// Trade: template 1, 80 bytes. Constant fields omitted: PriceExponent (PriceExponent).
struct Trade {
    static constexpr bool kIsStruct = true;
    static constexpr uint16_t kTemplateId = 1;
    static constexpr size_t kSize = 80;
    uint16_t blockLength;
    uint16_t templateId;
    uint16_t schemaId;
    uint16_t version;
    uint32_t InstrumentId;
    uint16_t VenueId;
    uint16_t Flags;
    uint64_t Sequence;
    uint64_t InstrumentSeq;
    uint64_t ExchangeTs;  // Ts
    uint64_t RecvTs;  // Ts
    int64_t Price;  // Price
    int64_t Qty;  // Qty
    uint64_t TradeId;
    uint8_t Aggressor;  // Side
    uint8_t _pad0[7];
};
static_assert(sizeof(Trade) == Trade::kSize);
static_assert(offsetof(Trade, blockLength) == 0);
static_assert(offsetof(Trade, templateId) == 2);
static_assert(offsetof(Trade, schemaId) == 4);
static_assert(offsetof(Trade, version) == 6);
static_assert(offsetof(Trade, InstrumentId) == 8);
static_assert(offsetof(Trade, VenueId) == 12);
static_assert(offsetof(Trade, Flags) == 14);
static_assert(offsetof(Trade, Sequence) == 16);
static_assert(offsetof(Trade, InstrumentSeq) == 24);
static_assert(offsetof(Trade, ExchangeTs) == 32);
static_assert(offsetof(Trade, RecvTs) == 40);
static_assert(offsetof(Trade, Price) == 48);
static_assert(offsetof(Trade, Qty) == 56);
static_assert(offsetof(Trade, TradeId) == 64);
static_assert(offsetof(Trade, Aggressor) == 72);
```

## HTTP API (cold path)

`api_server.h`. Raw POSIX sockets, HTTP/1.1, read-only except reload. Bound to `api.host:api.port` (default 127.0.0.1:8080). Not on the hot path.

| Method and path | Returns |
|---|---|
| `GET /health` | `{"status":"ok"}` |
| `GET /status` | name, version, uptime, epoch, partitions, sessions, topics |
| `GET /topics` | per topic: name, `topic_id`, `degraded` (the count of its degraded partitions), `partitions` [{`partition_num`, `stream_id`, `state`}] and `sessions` (names of the sessions whose patterns take it); also inside `/status` as `topics` |
| `GET /partitions` | per partition: name (`<topic>/<n>`), `topic`, `topic_id`, `partition_num`, `stream_id`, state (init, active, degraded), last sequence, packets, messages, unknown templates, gaps, last receive ns, transport state (`unknown`, `up`, `down`) and the `transport_down` / `transport_up` counts, `stream_mismatch` and `last_stream_stamp`, `inst_gaps` and `inst_dups`; with a tap, `tap` {`ring`, `records`, `dropped`, `bytes` (ring head), `readers`, `lag_bytes` (the slowest reader)} |
| `GET /sessions` | per session: name, id, owner, ring path, epoch, published, filtered, filter generation, ring-wide `reader_flags`, `topics` (its patterns, as given) and `readers` (slot, pid, name, lag bytes, records, overruns, heartbeat, tracing) |
| `POST /sessions/<name>/trace?on=1\|0[&pid=N][&sample=N]` | latency tracing for the session's readers: every reader, or the slots of one pid; trace 1 record in `sample` |
| `GET /symbols?from=&max=` | id to name pairs |
| `GET /lvc/{symbol}?type=TRADE` | last value for a symbol (name or numeric id) and type: header fields plus `payload_hex` |
| `GET /topology` | what the agent consumes (one `data` entry per partition, local `{topic, partition, stream_id}`, one `recap` entry per recap block) and feeds (one ring per session, with the apps reading it; one `record` entry per tap ring), each by canonical endpoint key: `docs/context/observability.md` |
| `GET /metrics` | Prometheus text exposition (`docs/context/observability.md`: every partition, transport, recap, group and session counter, state gauges, the `recv_to_publish` histogram, `subplane_service_info`); `?format=json` for the JSON form |
| `POST /reload` | re-reads the config file and reconciles the `sessions` section; the reply lists `ignored` sections that changed (`registry`, `topics`, `agent.thread_groups`) |

Anything else is 404 with a JSON error body. There is no per-session detail path; `/sessions/<name>/trace` is the only per-session action.

## Observability

Counters are one-writer relaxed atomics (`core/counters.h`), read cold.

- Per partition: `packets`, `bytes`, `messages`, `unknown_templates`, `undecodable_tail`, `schema_mismatch`, `gaps`, `dups`, `admin`, `degraded_events`, `decode_errors`, `malformed`, `truncated`, `transport_errors`, `seq_resets`, `stream_mismatch`, `inst_gaps`, `inst_dups`, `tap_records`, `tap_dropped`, plus `last_unknown_template`.
- Per session: `published`, `filtered`, `publish_failed`, `ring_overruns` (reserved; the writer cannot observe reader overruns, so it stays 0: each reader reports its own through its reader slot, `subplane_app_overruns_total`).
- Histogram: one `LogHistogram` per thread group, `recv_to_publish` in ns (receive timestamp to after the packet is published, recorded per packet). 64 power-of-two buckets by 4 sub-buckets (256 total), relative error up to 25%, single writer with relaxed load and store, snapshotted cold for p50, p90, p99, p99.9, min, max, mean. Per-stage histograms are planned.
- Transports keep their own one-writer counters (`aeron`: fragments, oversize, image events, client errors; `tcp_stream`: connects, disconnects, framing errors), exported through the cold `Transport::counters()`.
- `/metrics` serves all of these in Prometheus format, with the metric names, labels and identity rules in `docs/context/observability.md`; `?format=json` gives the JSON form. The metrics manager can write periodic reports per `metrics.*` config.

## Configuration reference

`config.yaml`, walked section by section. Relative schema paths resolve against the config file's directory. Only `sessions` is reloadable. The top level also has `registry` and `topics`; see "Topics and partitions".

`agent`
- `name` (default `subplane`): used in ring file names, the default control socket path and logs.
- `version`: informational.
- `max_symbols` (default 65536): size of the symbol table, filter bitsets and last-value cache.
- `ring_mode` (default `"0644"`, octal): ring file mode. Readers of another uid need group write (`"0660"`) to claim a reader slot; without it they read but do not report.
- `thread_groups[]`: `name`, `cpu` (pin core, -1 = unpinned), `topics` (names; `partitions:` is an error). Omitted means one group `g0` with every topic. Each topic must be in exactly one group, and its partitions run in that group.

`registry`
- The path of the site's topic registry file (relative to the config file), or the entries inline as a sequence of `{id, name}`. Required.

`topics[]` (each partition is one activation; at most 64 partitions per agent)
- `name`: required, unique, in the registry. No `id:` key.
- `partitions[]`: optional; each entry is deep-merged over the keys below (maps merge, anything else replaces; `{}` = no overrides). Omitted means one partition. A partition entry has no `name`.
- The keys below are shared by the topic's partitions unless a partition overrides them.
- `transport`: `type` (`udp_multicast` or `udp`), `group` (empty = unicast), `port`, `interface` (local address, empty = any), `rcvbuf` (bytes), `busy_poll_us` (0 = off), `mtu`, `batch` (1 to 64), `kernel_timestamps` (default false), `multicast_loop` (default true), `reuse_port` (default true). `type: tcp_stream` and `type: aeron` take the keys shown under Transport.
- `framing`: the block described above. Absent means bare concatenation with length from the schema and no sequence.
- `decoder`: `type: sbe_routing`, `schema` (XML path), `fields` (`symbol`, `sequence`, `exchange_ts`, `venue`, and `stream_id`, read from admin messages only; each a root-block schema field name), `symbol_id_mode` (`table` | `passthrough`), `symbol_scope` (`global` | `venue`), `messages[]` (`template` plus `msg_type` or `admin`; recap markers also take `fields`).
- `heartbeat_timeout_ms` (default 2000), `dup_window` (default 1024).
- `lvc` (`on` default | `off`): whether this partition's data feeds the last-value cache. Turn it off for partitions whose values are built from many messages (book updates, accumulated volume), where the last single update would mislead; their state comes from recap (planned) instead.
- `recap`: the snapshot service for this partition; see "Recap" (config, component and manager implemented; the merge planned).
- `tap`: `{capacity_mb: 256, max_payload: 65536}` (both optional; the block's presence turns it on): copy every received unit into the partition's tap ring for `subplane record`; see "Tap".

`sessions[]` (reloadable)
- `name`, optional `owner` (config), `ring.capacity_mb` (or `ring.capacity_bytes`; power of two, at least 64 KiB), `ring.max_payload` (default 4096, at most capacity / 4), `topics` (patterns; omitted = all topics; `partitions` is an error), `symbols` (`["*"]` or names or numeric ids), `msg_types` (names), `deliver.bus_admin`, `deliver.agent_control` (both default true), `recap` (default false; see "Recap").

`fanout`
- `ring_dir` (default `/dev/shm`). Only the `shm_ring` fan-out exists; there is no `type` key yet. `aeron_ipc` is planned.

`control`
- `socket_path`, `fallback_socket_path`, `linger_ms` (default 5000).

`lvc`
- `max_payload` (default 512).

`api`
- `enabled` (default true), `host`, `port`.

`metrics`
- `enabled`, `report_interval_seconds`, `output_format`, `console_output`.

`logging`
- `level`, `console_output`, `output_format`.

`observability` (optional)
- `site`, `env`, `labels` (a map): identity carried by `subplane_service_info` (`observability.md`). Invalid label names fail startup.

Startup is fail-fast: an unknown component type, a bad schema, a bad framing block, an invalid decoder mapping, or a thread-group assignment error stops the agent with a message. A failure to open a session or bind the control socket is logged and the agent keeps running.

## Source layout

```
include/subplane/
  core/        message.h filter_table.h seqlock.h histogram.h counters.h clock.h spsc_queue.h
  registry/    component_registry.h
  transport/   transport.h udp_multicast.h tcp_stream.h aeron_transport.h
  recap/       recap_transport.h aeron_recap.h recap_config.h recap_manager.h
  framing/     framing.h sbe_length.h
  decoder/     decoder_common.h sbe_schema.h sbe_routing_decoder.h dbn_decoder.h flatbuffers_decoder.h flatbuffers_wire.h sbe_views.h sbe_codec.h
  pipeline/    pipeline.h partition.h topic_registry.h session.h session_manager.h lvc.h symbol_resolver.h recap_state.h
  shm/         ring_layout.h control_events.h        (shared with apps)
  fanout/      ring_writer.h
  control/     control_proto.h control_server.h      (proto shared with apps)
  client/      shm_reader.h control_client.h         (the whole app SDK)
  proto/       subplane_proto_codec.h                (SPP codec, generated from schemas/subplane_proto.xml, checked in)
  record/      pcapng.h                              (recordings: pcap/pcapng reader and writer, docs/context/recording-format.md)
  logging/ metrics/                                  (cold path)
  agent.h api_server.h config_manager.h symbol_table.h status_types.h types.h persistence_manager.h
src/            mirrors include/; src/main.cpp builds the `subplaned` binary; src/registry/builtin_components.cpp holds the static tables
src/transport/parked/   Kafka producer code kept for reference, not built
tools/          subplane.cpp (CLI dispatcher) probe.cpp tail.cpp schema.cpp send.cpp replay.cpp dbn_sim.cpp aeron_pub.cpp soak.cpp commands.h (+ shared headers)
examples/       shm_demo_app.cpp (resident app and short-lived worker) rule_runner.cpp (function-style runtime) views_app.cpp (canonical struct views) canonical_pub.cpp (sample publisher)
schemas/        sample_md.xml (referenced by config.yaml) subplane_canonical.xml (the canonical flat schema) subplane_proto.xml (SPP, docs/protocol/) CMakeLists.txt (views generated at build time)
tests/          unit/ integration/ data/ fixtures/flatbuffers/ (real flatc output; README explains regeneration)
third_party/    pugixml, doctest (vendored)
scripts/        dev.sh (build, test, run in the toolchain image) deps-2604.sh fetch-cme-sample.sh merge-pr.sh
docker/         Dockerfile (the toolchain image: Ubuntu 26.04, GCC 16)
cmake/          aeron.cmake (FetchContent of the pinned Aeron tag)
```

`persistence_manager` is compiled into the core library but not wired into the pipeline; it is off the hot path and not used today.

## Tools

One CLI binary, `subplane <command>`, built by default (`SUBPLANE_BUILD_TOOLS`, target `subplane`, `tools/subplane.cpp` dispatching to one file per command). `subplane --help` lists the commands; `subplane <command> --help` the options. The example apps (`shm_demo_app`, `rule_runner`, `views_app`, `canonical_pub`) stay separate binaries because their point is what they do not depend on.

- `subplane dbn-sim --port N (--file x.dbn | --synthetic) [--rate rec/s] [--clients N] [--prelude dbn|none] [--loop N] [--timing file] [--instruments N] [--count N] [--mix mbo=60,trade=30,mbp1=10] [--gap-every N] [--heartbeat-ms N]`: TCP server that streams DBN like Databento's live gateway, from an uncompressed `.dbn` file or synthetic v3 records (MBO, trade, MBP-1, SYSTEM heartbeats).
- `subplane send --port N [--schema X] [--framing '<yaml>'] [--group G | --host H] [--rate msgs/s] [--count N] [--symbols N] [--per-packet N] [--mix trade=70,quote=25,book=5] [--heartbeat-ms N] [--gap-every N] [--seq-reset-at N] [--unknown-every N] [--dup-every N]`: synthetic publisher using the same schema and framing code as the agent. Can inject gaps, a sequence reset, unknown templates and duplicates.
- `subplane aeron-pub --channel aeron:ipc --stream-id 1001 [--aeron-dir DIR] [--count N] [--rate msg/s] [--symbols N] [--heartbeat-every N] [--embedded-driver] [--driver-timeout-ms N]`: canonical Trade/Quote/Heartbeat over an Aeron exclusive publication, one message per offer, one Sequence counter across all of them. `--embedded-driver` starts a media driver in-process so a UDP test needs no `aeronmd`.
- `subplane probe --port N [--schema X] [--framing '<yaml>'] [--group G] [--interface IP] [--packets 100] [--timeout-ms 5000] [--verbose]`: validate a framing block against live packets.
- `subplane schema --schema X.xml --views cpp|numpy|q|all --out DIR [--namespace NS] [--message M[,M...]] [--stdout]`: typed views for flat SBE messages (see Client SDK); prints emitted and skipped messages.
- `subplane tail <ring path> [--count N] [--follow] [--from-start] [--hex]`: dump ring records (header fields, control event bodies).
- `subplane replay --pcap FILE --port N [--host H] [--rate pps|0] [--timing pcap] [--loop N]`: replay UDP payloads from a classic pcap or a pcapng file, subplane recordings included (`include/subplane/record/pcapng.h`, no libpcap). `subplane replay --bench --config config.yaml [--packets N] [--per-packet K] [--symbols N] [--sessions K] [--batch N] [--cpu N] [--json]`: drive the pipeline in-process with an in-memory transport and print the `recv_to_publish` histogram.
- `subplane soak --role writer|reader|harness [...]`: ring chaos test with CRC payloads, N readers, writer kill and restart; checks readers see `WriterGone` then `AGENT_RESTARTED`.
- `shm_demo_app --socket PATH [--session NAME] [--symbols 1,2,3] [--trades-only] [--seconds N] [--quiet]` (in `examples/`): the whole app contract in one file.
- `canonical_pub --port N [--host H | --group G] [--rate msg/s] [--count N] [--symbols N]` (in `examples/`): the smallest publisher the agent can ingest, about a hundred lines with no agent code: fills the generated canonical structs, applies the framing block from `config.yaml`'s `canonical` partition, sends over UDP or multicast. The file a gateway author copies; real gateways (crypto WebSocket, FIX) live in the `subplane-feeds` repository (planned). `ctest -R canonical_pub` runs it through the agent to `views_app`.
- `views_app <ring path> [--count N] [--seconds N] [--quiet] [--from-start]` (in `examples/`): reads a ring of canonical messages through the generated struct views, no codec; prints one line per message and a summary (`trades= quotes= books= snapshots= other= heartbeats= bad=`), exit 1 if any data record was rejected by every view.

## Build and test

```
scripts/dev.sh test       # in the toolchain image: cmake -S . -B build, build, ctest
```

Dependencies (all in the image, `scripts/deps-2604.sh`): GCC 16 (CMake refuses older), CMake 3.20+ (3.30+ with Aeron), pkg-config, yaml-cpp, pthreads, git and network access for the Aeron fetch. pugixml and doctest are vendored. Aeron (C client + C media driver, pinned tag in `cmake/aeron.cmake`, no Java) is fetched and built at configure time; `-DSUBPLANE_WITH_AERON=OFF` builds the agent without the `aeron` transport and without the fetch, and `-DFETCHCONTENT_SOURCE_DIR_AERON=<checkout>` builds from a local or vendored tree. `libuuid` (`uuid-dev`) is optional for the driver. Flags: `-O2`, and `-Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion -Werror` on first-party code. Options: `SUBPLANE_BUILD_TESTS`, `SUBPLANE_BUILD_TOOLS`, `SUBPLANE_WERROR`, `SUBPLANE_WITH_AERON`, `SUBPLANE_AERON_TAG`.

ctest targets: `test_core`, `test_core_types`, `test_ring`, `test_sbe`, `test_transport`, `test_framing`, `test_pipeline`, `test_session_manager`, `test_control` (doctest unit tests), and `sender_probe` (sender to probe over unicast loopback for two framings), `ring_soak_short` (2 s, 2 readers), `replay_bench` (in-process bench smoke test), `dbn_sim`, `flatbuffers_replay`, `test_canonical`, `canonical_views_app`, `canonical_pub`, `schema_views_numpy`, `cme_probe` (skipped without the sample), and `aeron_ipc` / `aeron_udp` (present when built with Aeron).

`scripts/dev.sh` builds `docker/Dockerfile` (Ubuntu 26.04 pinned by digest, `scripts/deps-2604.sh`) under a tag hashed from those two files, and runs every build, test and tool in it as the calling user, with `--network host` and `--shm-size=2g`; `dev.sh bench` adds `SYS_NICE` and drops seccomp for pinned SCHED_FIFO runs. `.github/workflows/ci.yml` runs `deps-2604.sh` on the same pinned base on push and pull request. The examples build as C++20, which holds the app SDK there (INV-007). `schema_views_numpy` needs `python3-numpy`, which the image has; the image also has `promtool`, and `canonical_pub` lints the agent's `/metrics` with it (the feeds repo lints the gateway's, the snap's and an agent's with a recap partition).
