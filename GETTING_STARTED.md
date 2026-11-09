# Getting started with subplane

A 10-minute walkthrough on one machine, using unicast UDP so no multicast setup is needed. You will build the agent, feed it synthetic SBE traffic, look at the shared-memory ring, and run the demo app.

All commands run from the repository root. Run each long-lived process in its own terminal.

## 1. Build

```bash
scripts/dev.sh test        # builds ./build in the toolchain image, then runs ctest
```

The build runs in Docker, in the toolchain image (`docker/Dockerfile`: Ubuntu 26.04, GCC 16), built on first use. pugixml and doctest are vendored.

The binaries need Ubuntu 26.04's glibc and libstdc++. On any other host, put `scripts/dev.sh run` in front of each `build/...` command below, in every terminal: each run is a fresh container, but all of them share the host's network, `/dev/shm` and `/tmp`, so the agent in one terminal and `tail` or an app in another see the same rings and control socket, and `curl` works from the host. On an Ubuntu 26.04 host, `sudo scripts/deps-2604.sh` installs the same toolchain, plain `cmake` works, and the commands run as written.

## 2. Make a local config

`config.yaml` subscribes to multicast group 239.1.1.1. For a local run, switch to unicast (empty `group`) and make the schema path absolute:

```bash
sed -e 's/group: 239.1.1.1/group: ""/' -e "s#schema: schemas#schema: $PWD/schemas#" config.yaml > /tmp/subplane-demo.yaml
```

The config defines one partition `eq-p0` (UDP port 30001, framed as an 8-byte sequence plus 2-byte count packet header and a 2-byte length prefix per message) and one session `alpha` that wants TRADE and QUOTE for all symbols.

## 3. Start the agent (terminal 1)

```bash
build/subplaned --config /tmp/subplane-demo.yaml
```

Look for a `CONTROL LISTENING` log line. If it fails with `BIND_FAILED Address already in use`, something else holds port 8080: add `-e 's/port: 8080/port: 8089/'` to the `sed` in step 2 and use that port with `curl` below. The control socket is `/run/subplane/subplane.sock` if that directory is writable, otherwise the fallback `/tmp/subplane.sock`. The session ring appears at `/dev/shm/subplane.subplane.alpha.ring` (`subplane.<agent>.<session>.ring`).

## 4. Check framing with subplane probe (optional, terminal 2)

Before pointing the agent at a bus, `subplane probe` tells you whether a framing block matches the packets on the wire. It binds the same port as the agent, so stop the agent first if you try this locally.

```bash
FRAMING='{packet_header: [{name: seq, type: u64le}, {name: count, type: u16le}], message_prefix: [{name: len, type: u16le}], sequence: packet_header.seq, length: message_prefix.len}'
build/tools/subplane probe --schema schemas/sample_md.xml --framing "$FRAMING" --port 30001 --packets 50 &
build/tools/subplane send --schema schemas/sample_md.xml --framing "$FRAMING" --port 30001 --count 100 --per-packet 3
wait
```

It prints PASS or FAIL, the message mix, and any length mismatches or sequence gaps. Then restart the agent from step 3.

## 5. Send synthetic traffic (terminal 2)

```bash
FRAMING='{packet_header: [{name: seq, type: u64le}, {name: count, type: u16le}], message_prefix: [{name: len, type: u16le}], sequence: packet_header.seq, length: message_prefix.len}'
build/tools/subplane send --port 30001 --framing "$FRAMING" --rate 1000 --per-packet 3
```

`--rate` is messages per second. Useful extras: `--gap-every 50` (skip a sequence number), `--seq-reset-at N`, `--unknown-every N` (inject an unknown template), `--dup-every N`, `--heartbeat-ms 100`, `--symbols 100`, `--mix trade=70,quote=25,book=5`.

## 6. Look at the agent

```bash
curl -s localhost:8080/status
curl -s localhost:8080/partitions
curl -s localhost:8080/sessions
curl -s localhost:8080/symbols
curl -s 'localhost:8080/lvc/1?type=TRADE'     # last value for symbol "1"
curl -s localhost:8080/metrics                # counters and the recv_to_publish histogram
```

The partition should be `active` with a growing packet count. `published` on session `alpha` counts records written to its ring; `filtered` counts records its filter rejected (here BOOK_UPDATE, which `alpha` does not subscribe to).

## 7. Tail the ring

```bash
build/tools/subplane tail /dev/shm/subplane.subplane.alpha.ring --follow
```

Options: `--from-start`, `--count N`, `--hex`. You will see `AGENT_RESTARTED` first, then `SYMBOL_MAPPED` events as symbols are first seen, interleaved in order with `TRADE` and `QUOTE` records. Restart the sender with `--gap-every 50` and watch `GAP_DETECTED` appear in line with the data.

## 8. Run the demo app

```bash
build/examples/shm_demo_app --socket /tmp/subplane.sock --session demo --symbols 1,2,3 --trades-only --seconds 10
```

Use the socket path the agent logged. The app connects to the control channel, creates its own session `demo` with a filter, opens that session's ring, marks the live head, fetches the last-value snapshot, then tails the ring and de-duplicates the overlap by sequence. That loop is the whole app contract; see `examples/shm_demo_app.cpp`, which depends only on `subplane/client/shm_reader.h` and `subplane/client/control_client.h`.

Check `curl -s localhost:8080/sessions` while it runs: `demo` shows `owner: client`. When the app exits the session lingers for `control.linger_ms` (5 s) and is then destroyed.

## 9. Change sessions at runtime

Edit the `sessions:` section of `/tmp/subplane-demo.yaml` (for example add a symbol list or a message type), then:

```bash
curl -s -X POST localhost:8080/reload
build/tools/subplane tail /dev/shm/subplane.subplane.alpha.ring --follow    # a FILTER_RELOADED event appears in line with the data
```

Only the `sessions` section is reloadable; partitions, framing and decoder changes need a restart.

## 10. Benchmark the pipeline in-process

```bash
build/tools/subplane replay --bench --config /tmp/subplane-demo.yaml --sessions 2 --packets 200000
```

This drives the pipeline on the calling thread with an in-memory transport and prints the `recv_to_publish` histogram. Add `--cpu N` to pin and `--json` for machine-readable output. It measures the agent, not the kernel socket; see `docs/context/project-vision.md` for how to read the numbers.

## Next steps

- Run `build/examples/rule_runner --socket /tmp/subplane.sock --from-start --seconds 10` to see the function-style consumer, and start several `shm_demo_app --seconds 10` instances at once to see short-lived sessions come and go in `/sessions`.
- Then pick the one guide for what you are doing (`docs/README.md` has the map):
  - a real deployment from a reference config (crypto exchanges, a firm SBE bus): `docs/guides/deploy-reference.md`;
  - your own app: `docs/guides/app-developer.md`;
  - your own bus, publisher or schema: `docs/guides/custom-integration.md`;
  - running and monitoring it: `docs/guides/operator.md`.
