# flashtick

A single-binary routing agent (`flashtickd`) for an internal SBE bus. One instance runs on each application box. It subscribes to the bus over UDP multicast, finds message boundaries from the published SBE schema, decodes only the routing fields, filters per application, and fans the original SBE bytes out through shared-memory rings. Apps decode SBE with their own codecs and stop doing deserialization, filtering and routing.

It is not a decoder and not a broker. The only hard requirement on a publisher is its SBE schema.

```
firm feed handler -> internal bus (SBE) -> flashtick (per box) -> shared-memory ring per app session -> apps
```

## Quick start

Needs a C++20 compiler (GCC 11 or newer), CMake 3.20+, pkg-config and yaml-cpp (`libyaml-cpp-dev` on Ubuntu).

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
(cd build && ctest --output-on-failure)

# Local demo config: unicast instead of multicast, so no network setup is needed.
sed -e 's/group: 239.1.1.1/group: ""/' -e "s#schema: schemas#schema: $PWD/schemas#" config.yaml > /tmp/ft.yaml

build/flashtickd --config /tmp/ft.yaml &

# Synthetic publisher, using the same framing as config.yaml.
FRAMING='{packet_header: [{name: seq, type: u64le}, {name: count, type: u16le}], message_prefix: [{name: len, type: u16le}], sequence: packet_header.seq, length: message_prefix.len}'
build/tools/ft_sbe_sender --port 30001 --framing "$FRAMING" --rate 1000 --per-packet 3 &

curl -s localhost:8080/status
build/tools/ft_tail /dev/shm/flashtick.flashtick.alpha.ring --from-start --count 10
build/examples/shm_demo_app --socket /tmp/flashtick.sock --session demo --seconds 5
```

The control socket is `/run/flashtick/flashtick.sock` if that directory is writable, otherwise `/tmp/flashtick.sock`; the agent logs which one it bound. `GETTING_STARTED.md` walks through this step by step.

## Consumption models

One contract serves three kinds of consumer. Each has a runnable example that depends only on the two client headers.

| Workload | What the agent gives it | Example |
|---|---|---|
| Resident app (strategy, pricer) | A filtered session, snapshot for late join, ordered ring with in-band gap and restart events | `examples/shm_demo_app.cpp` |
| Elastic or short-lived worker (burst jobs, pods, scale-to-zero, blue/green) | Per-box costs paid once and kept warm; a new instance is one control round trip plus a ring file, live with state in milliseconds | `shm_demo_app --seconds N`, several at once |
| Function-style runtime (alerting rules, per-symbol shards, side channels) | Filter as trigger spec, ring as a replayable event log, snapshot as initial context | `examples/rule_runner.cpp` |

```bash
# elastic workers: three short-lived sessions side by side
for i in 1 2 3; do build/examples/shm_demo_app --socket /tmp/flashtick.sock --session w$i --trades-only --quiet --seconds 10 & done
curl -s localhost:8080/sessions

# function runtime: dry-run the rules on ring history, then go live
build/examples/rule_runner --socket /tmp/flashtick.sock --from-start --seconds 10
```

`docs/context/project-vision.md` ("Consumption models") explains each case and its current limits. Transports, framing and routing are agent configuration, not application code: apps never learn which bus they are on.

## Documentation

- `GETTING_STARTED.md`: a 10-minute hands-on walkthrough.
- `docs/context/project-vision.md`: identity, thesis, latency budget, non-goals, roadmap.
- `docs/context/architecture.md`: data flow, ring and record format, control channel, configuration reference, source layout, tools.
- `docs/context/invariants.md`: the non-negotiable engineering rules.
- `docs/context/landscape.md`: how flashtick compares with agents, libraries and middleware.

Everything in the docs is implemented unless marked "(planned)".

## Checked against real packets

`scripts/fetch-cme-sample.sh` downloads 27 public CME MDP 3.0 packets and CME's schema (v9 and v13), and `ctest -R cme_probe` replays them through the production framer and schema walker with a framing block written from CME's documentation. Every message length from the schema walk matches the wire. The test skips when the sample has not been fetched.

A second encoding over a second transport: `decoder: {type: dbn}` with `transport: {type: tcp_stream}` consumes Databento DBN records (Databento's live gateway framing: prelude, metadata, records). `build/tools/ft_dbn_sim --port 9550 --synthetic` serves synthetic records, or `--file x.dbn` replays a real file; `ctest -R dbn_sim` runs the synthetic version end to end. The commented `db-p0` partition in `config.yaml` is the configuration.

A third encoding with no library: `decoder: {type: flatbuffers}` routes FlatBuffers by field id straight off the wire and delivers the whole buffer, size prefix included. `ctest -R flatbuffers` runs real `flatc` output (`tests/fixtures/flatbuffers/`) through the decoder and over UDP through the agent. The commented `fb-p0` partition in `config.yaml` is the configuration.

## Typed views without a codec

A flat SBE message (no groups, no varData) is a fixed block behind the 8-byte message header. `ft_schema` turns the schema the agent loads into typed access to that block in three languages, so an app never links a codec or runs a code generator of its own:

```bash
build/tools/ft_schema --schema schemas/sample_md.xml --views all --out out/
# wrote out/sample_md_views.h  out/sample_md_views.py  out/sample_md_views.q
# emitted Trade, Quote, Ticker, Heartbeat, SeqReset; skipped BookUpdate: has groups (Levels)
```

**C++.** A message whose fields are all naturally aligned becomes a plain struct, and the `static_assert`s are the proof that it is the wire layout. A message with a misaligned field (SBE does not pad) becomes a view class with `memcpy` accessors instead. There is never a `#pragma pack`.

```cpp
// out/sample_md_views.h (generated)
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
static_assert(offsetof(Ticker, SeqNum) == 16);
// ...
class Trade {               // Price sits at offset 34: not aligned, so a view, not a struct
public:
    explicit Trade(const uint8_t* p) noexcept;
    int64_t Price() const noexcept;   // memcpy from offset 34
    uint32_t Qty() const noexcept;
    uint8_t Side() const noexcept;    // SideEnum
    // ...
};
```

Reading a ring with them, with `client/shm_reader.h` as the only SDK header:

```cpp
#include "flashtick/client/shm_reader.h"
#include "sample_md_views.h"
using namespace ft_views;

ShmReader rd;
rd.open("/dev/shm/flashtick.flashtick.alpha.ring");
RecordView rv;
while (rd.poll(rv) == ReaderStatus::Ok) {
    if (rv.hdr->kind != static_cast<uint8_t>(Kind::DATA)) continue;
    if (const Ticker* t = view<Ticker>(rv.payload, rv.payload_len)) {        // struct: pointer into the ring
        printf("%.8s seq=%llu\n", t->Symbol, (unsigned long long)t->SeqNum);
    } else if (auto tr = view<Trade>(rv.payload, rv.payload_len)) {        // view: accessors
        printf("sym=%u px=%lld qty=%u\n", rv.hdr->symbol_id, (long long)tr->Price(), tr->Qty());
    }
    if (!rv.valid()) { /* overwritten while reading: drop and resync */ }
}
```

`view<T>` returns null unless the payload is long enough and its header carries `T`'s template id and the schema id, so a wrong cast is impossible rather than merely unlikely.

**Python.** One frozen dataclass per message, standard library only, and `parse` for a whole ring record:

```python
from sample_md_views import Trade, parse

hdr, msg = parse(record)                 # record = 64-byte header + payload, as read from a ring or a recording
if isinstance(msg, Trade):
    print(hdr.symbol_id, msg.Price, msg.Qty, msg.Side)

t = Trade.from_bytes(payload)            # or just the SBE message
```

With NumPy installed each class also carries a `DTYPE`, so a buffer of payloads is a column store in one line: `np.frombuffer(payloads, dtype=Trade.DTYPE)['Price']`.

**q.** A `1:` fixed-width spec and a parse function per message:

```q
\l sample_md_views.q
.ft.sample_md.Trade.spec                    / ("hhhhjjjhjix";2 2 2 2 8 8 8 2 8 4 1)
t:.ft.sample_md.Trade.parse payload         / `blockLength`templateId`...`Price`Qty`Side!...
t`Price
```

Unsigned fields widen to the signed q type of the same width; char arrays are cut from the raw bytes. The q output is generated and checked structurally but has not been run under q here.

The sample schema's views are regenerated and compiled at every build (`ctest -R sbe_views`), so a layout regression fails the build. Little-endian schemas only.

## Licence

Apache License 2.0. See `LICENSE` and `NOTICE`; contribution guidelines are in `CONTRIBUTING.md`.
