# Operator guide

You deploy and run `subplaned` (one per application box) and the gateways that feed it, and you watch them. This guide covers the tasks; every key and metric is listed in the reference (`docs/context/architecture.md`, "Configuration reference"; `docs/context/observability.md`).

## 1. Run the agent

```bash
subplaned --config /etc/subplane/config.yaml
```

A config has five parts you will touch:
- **`agent`:** `name`, which appears in ring file names, the control socket path and every metric; `thread_groups`, which pin pipeline threads to cores.
- **`partitions`:** what to subscribe to, one entry per bus partition: its transport (UDP multicast, TCP, Aeron), framing and schema. These usually come from the publisher's team as a template.
- **`sessions`:** subscriptions defined by you rather than by apps. `POST /reload` applies changes without a restart.
- **`api`:** the HTTP port for status, metrics and topology.
- **`observability`:** `site`, `env` and labels that identify this box on dashboards.

Startup is fail-fast: a bad schema, framing block or mapping stops the agent with a message.

**Shared memory.** Each session's ring is a file in `/dev/shm` (`fanout.ring_dir`), sized by `ring.capacity_mb` plus 36 KiB. The agent reserves the whole file at creation. If `/dev/shm` is too small, that session fails with "No space left on device"; for a config session, the agent does not start. In containers, size `/dev/shm` (`--shm-size`) to the sum of your rings plus headroom.

**Apps under other users.** By default ring files are `0644`. Apps under the agent's user can read and report their lag. Apps under another user can read but cannot report, unless you set `agent.ring_mode: "0660"` and put them in the agent's group.

## 2. Run a gateway (crypto feeds)

Gateways (`spwsgw`, repo `subplane-feeds`) connect to an exchange and publish into the agent over Aeron:

```bash
spwsgw --pack packs/binance-spot.yaml --instruments site.yaml --channel aeron:ipc --stream-id 1001 --http 127.0.0.1:9101
```

`--http` turns on the same `/metrics`, `/topology` and `/health` the agent serves. `--instance`, `--site`, `--env` and `--labels` set its identity.

## 3. Watch it

Both the agent and the gateway serve these routes:

| Route | Use |
|---|---|
| `GET /metrics` | Prometheus scrape target (`?format=json` for humans) |
| `GET /topology` | what this service reads from and feeds, and which apps read each ring |
| `GET /health` | liveness probe; a gateway answers 503 while it is disconnected from its exchange |

The agent also serves `GET /status`, `/partitions` and `/sessions` as JSON.

Point one Prometheus job at every agent and gateway; every series carries `service`, `instance` and `host`. Alerts worth having from day one:

| Alert | Query | Meaning |
|---|---|---|
| silent feed | `subplane_partition_last_receive_age_seconds > 5` | nothing arriving on a partition |
| bus loss | `rate(subplane_partition_gaps_total[5m]) > 0` | messages lost upstream of the agent |
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
| `subplane_partition_messages_total` flat | `subplane_transport_state`, the publisher, multicast routing (`subplane probe`) |
| an app appears in `/topology` without `reader` | it was built with an older SDK, or cannot write the ring file (`agent.ring_mode`) |
| `subplane_reader_slots_used` near 64 per session | more than 64 readers on one ring; the extras read but do not report |
| `scan.unreadable` high in `/topology` | the agent cannot read other users' `/proc/<pid>/maps`: run it with `CAP_SYS_PTRACE`, or rely on reader slots |
