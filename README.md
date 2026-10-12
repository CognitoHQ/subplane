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
