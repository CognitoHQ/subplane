# FlatBuffers fixture

Real `flatc` output for the `flatbuffers` decoder tests, so the agent build
never depends on the FlatBuffers library.

- `md.fbs`: a bus-like schema. Root `Envelope` with explicit field ids, a
  `Body` union (`Trade`, `Quote`, `Heartbeat`), the instrument two tables deep
  with a string symbol, a `flags` field "added in v2" that old producers never
  set, file identifier `MDv1`.
- `gen_fixture.cpp`: builds 300 size-prefixed messages with the generated
  builders and writes the three outputs below. Every 5th message omits
  `flags`, so its vtable is shorter than the schema's.
- `fb_sample.bin`: the 300 buffers back to back (each `u32 size` + buffer).
- `fb_sample.pcap`: the same buffers as UDP/IPv4/Ethernet datagrams, 1..4 per
  datagram, for `ft_replay --pcap`.
- `fb_sample.expected`: one line per message: `kind seq ts instrument venue symbol flags`
  (kind 1 trade, 2 quote, 3 heartbeat; instrument/venue 0 and symbol `-` for heartbeats).

Regenerate (host only; outputs are committed, generated with flatc 2.0.8):

```sh
cd tests/fixtures/flatbuffers
/usr/bin/flatc --cpp md.fbs
g++ -std=c++20 -O2 -Wall gen_fixture.cpp -o gen_fixture && ./gen_fixture
rm -f gen_fixture md_generated.h
```

Routing config the tests use (field ids from `md.fbs`): `body_type` is id 2 of
`Envelope`; `body` is id 3; inside `Trade`/`Quote` the `instrument` is id 0;
inside `Instrument`, `id` is 0, `venue` 1, `symbol` 2; `seq` 0 and `ts` 1 on
`Envelope`.
