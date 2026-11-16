# Operator guide

You deploy and run `subplaned` (one per application box) and the gateways that feed it, and you watch them. This guide covers the tasks; every key and metric is listed in the reference (`docs/context/architecture.md`, "Configuration reference"; `docs/context/observability.md`).

## 1. Run the agent

```bash
subplaned --config /etc/subplane/config.yaml
```

A config has five parts you will touch:
- **`agent`:** `name`, which appears in ring file names, the control socket path and every metric; `thread_groups`, which pin pipeline threads to cores (they list topics).
- **`registry` and `topics`:** what to subscribe to. `registry` is the site's file of topic names and ids (shared with the publishers); `topics` has one entry per topic, with its transport (UDP multicast, TCP, Aeron), framing and schema. A topic that has several streams lists them under `partitions:`, each overriding what differs (a partition is one stream with one publisher and one sequence); with none, it is one partition. These usually come from the publisher's team as a template. `agent.thread_groups[].topics` assigns topics to threads.
- **`sessions`:** subscriptions defined by you rather than by apps, each with `topics:` patterns (`*` one segment, a final `>` the rest; omitted means all topics). `POST /reload` applies changes without a restart.
- **`api`:** the HTTP port for status, metrics and topology.
- **`observability`:** `site`, `env` and labels that identify this box on dashboards.

Startup is fail-fast: a bad schema, framing block, mapping or registry stops the agent with a message. The old top-level `partitions:` is refused with an error naming `topics:`.

**What a reload does.** `POST /reload` applies only `sessions:`. If `registry`, `topics` or `agent.thread_groups` changed, the reply says so (`{"status":"reloaded","ignored":["topics"]}`) and the agent logs `RELOAD_IGNORED`; those edits take effect on restart. Adding a partition takes a restart: add it on the primary and the DR agent, then move tickers.

**Shared memory.** Each session's ring is a file in `/dev/shm` (`fanout.ring_dir`), sized by `ring.capacity_mb` plus 36 KiB. The agent reserves the whole file at creation. If `/dev/shm` is too small, that session fails with "No space left on device"; for a config session, the agent does not start. In containers, size `/dev/shm` (`--shm-size`) to the sum of your rings plus headroom.

**Apps under other users.** By default ring files are `0644`. Apps under the agent's user can read and report their lag. Apps under another user can read but cannot report, unless you set `agent.ring_mode: "0660"` and put them in the agent's group.

## 2. Run a gateway (crypto feeds)

Gateways (`spwsgw`, repo `subplane-feeds`) connect to an exchange and publish into the agent over Aeron:

```bash
spwsgw --pack packs/binance-spot.yaml --instruments site.yaml --channel aeron:ipc --topic crypto.binance-spot --topic-registry site/topics.yaml --http 127.0.0.1:9101
```

The stream id is the partition's: topic id 11 (`crypto.binance-spot` in the site registry), partition 1, is `11 << 16 | 1 = 720897`, and the agent's Aeron transport derives the same number unless the config sets `stream_id`. `--http` turns on the same `/metrics`, `/topology` and `/health` the agent serves. `--instance`, `--site`, `--env` and `--labels` set its identity.

## 3. Watch it

Both the agent and the gateway serve these routes:

| Route | Use |
|---|---|
| `GET /metrics` | Prometheus scrape target (`?format=json` for humans) |
| `GET /topology` | what this service reads from and feeds, and which apps read each ring |
| `GET /health` | liveness probe; a gateway answers 503 while it is disconnected from its exchange |

The agent also serves `GET /status`, `/topics`, `/partitions` and `/sessions` as JSON. `/topics` is the view per topic: `name`, `topic_id`, `degraded` (how many of its partitions are degraded), its `partitions` (`partition_num`, `stream_id`, `state`) and the `sessions` whose patterns take it. `/partitions` is the operator's view of each stream, named `<topic>/<n>`: `topic`, `topic_id`, `partition_num`, `stream_id`, state, last sequence, counters (`gaps`, `dups`, `malformed`, ...), transport state, `stream_mismatch` and `last_stream_stamp`. Apps see topics only; partitions appear here, in the metrics (`topic` and `partition` labels) and in `/topology`.

Point one Prometheus job at every agent and gateway; every series carries `service`, `instance` and `host`. Alerts worth having from day one:

| Alert | Query | Meaning |
|---|---|---|
| silent feed | `subplane_partition_last_receive_age_seconds > 5` | nothing arriving on a partition |
| wrong stream | `rate(subplane_partition_stream_mismatch_total[5m]) > 0` | a publisher stamps a heartbeat with another partition's stream id; check its `--topic` (or `--stream-id`) and the agent's config. Data is not dropped; `last_stream_stamp` in `/partitions` shows the stamp |
| bus loss | `rate(subplane_partition_gaps_total[5m]) > 0` | messages lost upstream of the agent |
| instrument gap | `rate(subplane_partition_inst_gaps_total[5m]) > 0` | on a topic that maps `instrument_sequence`, an instrument's count jumped forward: a record of that instrument was lost on every partition. Apps got `INSTRUMENT_GAP` and recap sessions re-recapped the symbol. `inst_dups` counts records dropped as duplicates (normal when a record is sent on two partitions or replayed) |
| app falling behind | `subplane_app_lag_bytes > 0.5 * on(session) group_left subplane_session_ring_capacity_bytes` | an app is half a ring behind and will overrun |
| app stopped reading | `subplane_app_heartbeat_age_seconds > 5` | an app is hung or stopped polling |
| app losing data | `rate(subplane_app_overruns_total[5m]) > 0` | an app was lapped |
| nobody reads a session | `subplane_session_consumers == 0` | a ring is written for no one |
| gateway lost its exchange | `subplane_gateway_connected == 0` | no data from that venue |
| agent not consuming a gateway | `rate(subplane_gateway_dropped_total{reason="not_connected"}[1m]) > 0` | the gateway publishes, nobody subscribes |

## 4. Find out who reads what

`GET /topology` on an agent lists each session's ring and the apps reading it:
- with pid, user and name (the app's `SUBPLANE_APP_NAME`, else its systemd unit or process name);
- with how the agent found it (`via`: control socket, memory mapping, reader slot);
- with its live lag, heartbeat and overruns when the app reports through a reader slot.

Apps need no code for any of this. Endpoint keys in the documents join up across services, so a gateway's downstream key equals the agent's upstream key for the same stream.

## 5. Measure an app's read latency

Latency tracing is off by default and costs the app nothing until you turn it on:

```bash
curl -X POST 'http://127.0.0.1:8080/sessions/mm/trace?on=1'             # every reader of session mm
curl -X POST 'http://127.0.0.1:8080/sessions/mm/trace?on=1&pid=4242'    # one process
curl -X POST 'http://127.0.0.1:8080/sessions/mm/trace?on=1&sample=100'  # 1 record in 100
curl -X POST 'http://127.0.0.1:8080/sessions/mm/trace?on=0'
```

Within a moment, `subplane_app_read_latency_seconds{session, app, pid}` shows the time from the agent receiving a record to the app reading it. The histogram covers only traced records. Agent and app share the box's clock, so no clock sync is needed. `subplane_app_tracing` confirms which apps applied it; apps built with an older SDK do not trace.

## 6. Troubleshooting

| Symptom | Check |
|---|---|
| agent exits at start | its last log line names the bad key, schema or mapping |
| a session fails with "No space left on device" | `/dev/shm` size against the sum of ring capacities |
| `subplane_partition_messages_total` flat (per `topic` and `partition`) | `subplane_transport_state`, the publisher, multicast routing (`subplane probe`) |
| an app appears in `/topology` without `reader` | it was built with an older SDK, or cannot write the ring file (`agent.ring_mode`) |
| `subplane_reader_slots_used` near 64 per session | more than 64 readers on one ring; the extras read but do not report |
| apps report `SYMBOL_UNRESOLVED`, or recap of an unseen ticker goes to every partition | the topic's `resolver` in `/partitions` (on `<topic>/1`): `resolver_connected` 1, `resolver_lists` rising every `refresh_ms`, `resolver_instruments` as expected |
| `subplane_resolver_mismatch_total` rising | the agent's topic (registry id, number of partitions) differs from what the publisher's resolver says; fix whichever is wrong |
| `scan.unreadable` high in `/topology` | the agent cannot read other users' `/proc/<pid>/maps`: run it with `CAP_SYS_PTRACE`, or rely on reader slots |
