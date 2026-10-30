# Deploy a reference setup

Use this when you want subplane running with a known-good configuration and no design work. Each recipe below is complete: follow it top to bottom. Building something of your own (your bus, your schema, your gateway) is `custom-integration.md`.

| Recipe | You have | You get |
|---|---|---|
| A. Local demo | one Linux machine | synthetic SBE traffic through the agent to a demo app, in 10 minutes |
| B. Crypto exchanges | a box with internet access | live trades, quotes and books from Binance, Coinbase, Kraken, OKX or Bitstamp, in one canonical SBE schema, for every app on the box |
| C. Your firm's SBE bus | a multicast SBE bus and its schema XML | the bus routed to your apps, filtered per app |

**All recipes need:** Ubuntu 26.04 with GCC 16 (`sudo scripts/deps-2604.sh` installs it), or Docker on any Linux host, where `scripts/dev.sh` builds and runs in the toolchain image. Build once:

```bash
git clone <subplane repo> && cd subplane
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
# binaries: build/subplaned (agent), build/tools/subplane (CLI), build/examples/views_app (sample app)
```

## A. Local demo

Follow `GETTING_STARTED.md`. It runs the agent on `config.yaml` with plain UDP, sends synthetic traffic with `subplane send`, and reads it with the demo app.

## B. Crypto exchanges

```
exchange (WebSocket) -> spwsgw (one per exchange) -> Aeron IPC stream 1001 -> subplaned -> one ring per session -> your apps
```

**1. Build the gateway** (repo `subplane-feeds`; it also needs `libboost-dev` and `libssl-dev`):

```bash
git clone <subplane-feeds repo> && cd subplane-feeds
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
```

**2. Start the agent** with the reference config. It embeds the Aeron media driver, so nothing else needs to run:

```bash
sudo mkdir -p /run/subplane && sudo chown $USER /run/subplane   # control socket; it falls back to /tmp without this
build/subplaned --config deploy/crypto/agent.yaml
```

**3. Start one gateway per exchange.** Each publishes into the same stream; `--http` gives it metrics:

```bash
cd subplane-feeds
build/spwsgw --pack packs/binance-spot.yaml --symbols BTCUSDT,ETHUSDT --channel aeron:ipc --stream-id 1001 --http 127.0.0.1:9101
```

`--symbols` numbers instruments 1, 2, … in order. For stable ids across gateways and restarts, give each venue's instruments ids in one file and pass `--instruments site.yaml` (template: `site/instruments.example.yaml`). Other packs: `coinbase-exchange`, `kraken`, `okx`, `bitstamp`.

**4. Read it.** The reference config defines a session `all` (every instrument, every type):

```bash
build/examples/views_app /dev/shm/subplane.crypto.all.ring       # prints decoded canonical messages
```

Your own apps use the same ring, or create their own filtered sessions (`app-developer.md`).

**5. Check it works:**

```bash
curl -s localhost:8080/partitions     # "crypto": state active, messages increasing
curl -s localhost:9101/metrics | grep subplane_gateway_connected     # 1
curl -s localhost:8080/topology       # the gateway's stream upstream, apps reading the ring downstream
```

**Production notes:**
- **Cores:** pin the agent's pipeline (`thread_groups[].cpu` in `agent.yaml`) and each gateway (`taskset`, `--spin`) to isolated cores.
- **Ring size:** `capacity_mb: 64` holds a few seconds of all venues. Size `/dev/shm` for the sum of rings (Docker: `--shm-size`).
- **Apps under other users:** set `agent.ring_mode: "0660"` and add them to the agent's group, so they can report their lag.
- **Monitoring:** scrape `:8080/metrics` and each gateway's `--http` port. Alerts: `operator.md`, "Watch it".

## C. Your firm's SBE bus

You need three facts from the bus's owners: the schema XML, the multicast group and port of each partition, and the packet layout (header fields, length prefix). Then:

1. Copy `config.yaml` (it is a multicast SBE example) and change, per partition:
   - `transport.group`, `port`, `interface`;
   - `framing` (the packet layout);
   - `decoder.schema`;
   - `decoder.fields`: which schema fields hold the symbol, sequence, exchange time and venue;
   - `decoder.messages`: template id to type.
2. Check the framing against live packets before starting the agent:
   ```bash
   build/tools/subplane probe --schema bus.xml --framing '<the framing block>' --group 239.1.1.1 --port 30001 --packets 200
   ```
   It must print PASS.
3. Start `build/subplaned --config your.yaml`. Then add `sessions:` entries for your apps, or let apps create their own.

If the layout or the field mapping is not obvious, that is a custom integration: `custom-integration.md`, "Connect your own bus", explains each choice.
