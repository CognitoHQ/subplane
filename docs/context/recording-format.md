# Recording format

A subplane recording holds what an agent's partition **received**: each transport unit as it arrived, with its receive time. It does not hold what the agent made from it. Replay sends the units through an agent again, so the agent that replays rebuilds every ring record; nothing in a recording depends on the ring or record format.

A recording is a standard **pcapng** file ([IETF draft-ietf-opsawg-pcapng](https://datatracker.ietf.org/doc/draft-ietf-opsawg-pcapng/)), named `.pcapng`. There is no subplane file type: Wireshark, tshark, capinfos, editcap and tcpreplay open a recording as is, and `subplane replay --pcap` reads it (and classic pcap) today. Readers tell a subplane recording from any other capture by its CONTEXT block, not by its name.

Each file carries what is needed to read it: the partition's configuration and the schema it was decoded with. A recording can be read years later without asking which config was live that day.

What is built and what is planned:

- **Built:** the reader and writer (`include/subplane/record/pcapng.h`, header-only, C++20), pcapng input to `subplane replay --pcap`.
- **Built:** the agent's tap ring and its context file (`tap:` on a partition; architecture.md, "Tap").
- **Planned:** `subplane record`, and the `pcap` transport that replays a recording, or any pcap or pcapng capture, through an agent.

## Blocks

A file is one or more sections. Each section starts with a Section Header Block. The writer emits little-endian; the reader accepts either byte order per section.

| Block | Use |
|---|---|
| Section Header (SHB) | `shb_userappl` names the writer (`subplane record <version>`), `shb_os` the host OS. |
| Interface Description (IDB) | One per recorded partition. LINKTYPE_ETHERNET (1), snaplen 65535, `if_name` = the partition's name, `<topic>/<n>` (for example `crypto.binance-spot/1`), `if_tsresol` = 9 (nanoseconds). |
| Enhanced Packet (EPB) | One per received unit. The timestamp is the agent's receive time (`recv_ts`, CLOCK_REALTIME). Captured length = original length. |
| Custom (copyable, type `0x00000BAD`) | subplane's own blocks, below. Tools that rewrite files keep copyable custom blocks. |

The reader also accepts Simple Packet Blocks (no timestamp), the classic pcap formats (microsecond and nanosecond, either byte order), link types Ethernet (with 802.1Q/QinQ tags), Linux SLL and raw IP, and skips any other block by its length.

### The synthetic UDP wrapper

The agent sees UDP payloads, Aeron messages and TCP stream records, not frames. Each unit is stored inside a 42-byte Ethernet/IPv4/UDP header, so every pcap tool can read it:

- **Destination:**
  - for `udp` and `udp_multicast`, the partition's group (or 127.0.0.1) and port;
  - for `aeron`, the 32-bit stream id over address and port: 127.0.x.y with x.y its high 16 bits, port its low 16 (for the default `stream_id`, `topic_id << 16 | partition_num`, that is 127.0.x.y for the topic id and the partition number as the port), so no two streams share an address;
  - for `tcp_stream`, the configured port.
- **Source:** 0.0.0.0, because the agent does not learn the sender.
- **MAC addresses:** a multicast destination gets its `01:00:5e` MAC; any other address gets a locally administered one (`02:...`).
- **Checksums:** the IPv4 checksum is set; the UDP checksum is 0, which IPv4 allows.
- **Size limit:** a unit larger than one IPv4 datagram can hold (65,507 bytes) cannot be wrapped, and is counted rather than recorded.

## subplane custom blocks

Every subplane custom block has the body:

| Offset | Field | Type |
|---|---|---|
| 0 | Private Enterprise Number | u32, `kSpRecPen` |
| 4 | kind | u32, below |
| 8 | body length | u32, bytes that follow (before padding) |
| 12 | body | |

**The Private Enterprise Number is a placeholder.** It is 0, IANA's reserved value, until subplane registers its own PEN. Readers will keep accepting 0 after that, so earlier recordings still read.

| Kind | Name | Body |
|---|---|---|
| 1 | CONTEXT | a list of TLVs, below. At the head of every segment, before its first packet. |
| 2 | LOSS | `SpRecLoss` {u64 at_ns, u64 units, u64 bytes, u32 interface, u32 reserved}, 32 bytes: `units` units the partition received are not in the file (the tap ring lapped the recorder, or a unit did not fit the tap ring or one datagram). `bytes` is 0 when unknown. |
| 3 | TRANSPORT | `SpRecTransport` {u64 at_ns, u32 interface, u32 up}, 16 bytes: the partition's transport went down (0) or came up (1). |

Readers skip kinds they do not know. A new kind is an additive change.

### CONTEXT

A list of `{u16 tag, u16 reserved, u32 length, value, padding to 4}`. Unknown tags are skipped, so a new tag is an additive change and an old reader still reads the file.

| Tag | Name | Value |
|---|---|---|
| 1 | FORMAT_VERSION | u32, 1 |
| 2 | AGENT_NAME | text |
| 3 | AGENT_VERSION | text |
| 4 | AGENT_EPOCH | u64 ns: the agent start this segment came from |
| 5 | PARTITION | text: the partition name, `<topic>/<n>` (the IDB's `if_name`) |
| 6 | PARTITION_YAML | text: the partition's configuration as the agent loaded it |
| 7 | SCHEMA_XML | bytes: the schema the decoder loaded (`sbe_routing` only) |
| 8 | PARTITION_SHA256 | 32 bytes, of PARTITION_YAML |
| 9 | SCHEMA_SHA256 | 32 bytes, of SCHEMA_XML |
| 10 | TS_SOURCE | text: `kernel` (SO_TIMESTAMPNS) or `poll` (one clock read per receive batch) |
| 11 | TRANSPORT_TYPE | text: `udp`, `udp_multicast`, `tcp_stream` or `aeron` |
| 12 | INTERFACE | u32: the interface this context describes |
| 13 | WRAP | u32 IPv4 destination (host byte order), u16 destination port, u16 0: what each unit's synthetic UDP header carries |

Why a recording carries its context: the publisher's schema already versions every SBE message (schema id and version in its header), but the XML that was live is easy to lose, and the agent's framing and decoder config change over time. With both in the file, a replay uses the configuration the data was recorded with by default, and an explicit configuration can override it to test new config on old data (planned, with the `pcap` transport).

## Not covered, and why

- **Recording what one app saw** (a session's filtered ring, with gaps, overruns and recap) is planned separately. It is a forensic record of the output, which replaying the input does not reproduce exactly.
- **Native DBN files** for DBN partitions are planned. Until then their records travel in pcapng like any other unit.
- **Decoded rows** (export to columns for analysis) need the generic decoder (roadmap item 3).
- **The sender's address** is not recorded, because `PacketView` does not carry it.
