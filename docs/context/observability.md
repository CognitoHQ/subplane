# Observability contract

Every subplane service serves the same observability surface on its HTTP port, so one Prometheus, one Grafana and one topology collector cover the agent (`subplaned`), the gateways (`spwsgw`) and the services still to come (`spwsgw-snap`, the relay). This document is the contract. The `subplane-feeds` repository keeps an identical copy.

**Status:**
- The agent's `/metrics` and `/topology` are implemented.
- The gateway's (`spwsgw --http`) are implemented.
- The collector (`subplane topo`) is planned.

## Routes

| Route | Returns | Agent | Gateway |
|---|---|---|---|
| `GET /metrics` | Prometheus text exposition 0.0.4 (`text/plain; version=0.0.4`) | implemented | implemented |
| `GET /metrics?format=json` | the same counters as JSON, for humans and scripts | implemented | implemented |
| `GET /topology` | what the service consumes and what it feeds, `"schema": "subplane.topology/1"` | implemented | implemented |
| `GET /health` | 200, or 503 with a reason | implemented (always 200) | implemented: 503 while not connected to the exchange |

`?format=text` used to return an ad hoc line format. It now returns the Prometheus text, as the default does.

## Metric rules

- **Names:** `subplane_<area>_<name>`.
  - Counters end in `_total`.
  - Durations are in seconds and named `_seconds`, as are their histograms.
  - Sizes are in bytes and named `_bytes`.

  `promtool check metrics` passes on the agent's output.
- **Common labels** on every sample:
  - `service`: `subplaned`, `spwsgw`, …;
  - `instance`: `agent.name` for the agent;
  - `host`: from `gethostname()`.
- **Object labels:** `partition`, `session`, `group`, `state`; later `venue` and `endpoint`.
- **Identity:** `subplane_service_info{service, instance, host, version, epoch, site, env, <labels>} 1`. It carries the deployment labels, so a dashboard joins on it rather than repeating them on every series. `epoch` changes on every agent restart.
- **Histograms:** a `LogHistogram` (in ns) is exported as a Prometheus histogram in seconds.
  - There is one bucket per power-of-two major, at `le` = (2^(m+1) - 1) ns for m = 0..39 (1 ns up to about 18 minutes), plus `+Inf`. That makes 43 series per histogram instead of 256.
  - The edges are identical on every scrape and printed as exact decimals.
  - `+Inf` and `_count` are the sum of the buckets. The snapshot is not atomic across fields, and a `+Inf` below the last finite bucket would be invalid.

### Identity config (agent)
```yaml
observability:
  site: ny4          # optional
  env: prod          # optional
  labels:            # optional, added to subplane_service_info only
    team: md
```
Label names must match `[a-zA-Z_][a-zA-Z0-9_]*` and must not start with `__`; otherwise startup fails.

## The agent's metrics (implemented)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `subplane_service_info` | gauge | identity | always 1 |
| `subplane_uptime_seconds` | gauge | | seconds since start |
| `subplane_sessions` | gauge | | open sessions |
| `subplane_control_clients` | gauge | | clients on the control socket |
| `subplane_symbols`, `subplane_symbols_capacity` | gauge | | symbol table use and size (`agent.max_symbols`) |
| `subplane_lvc_writes_total` | counter | | last-value cache writes |
| `subplane_partition_<counter>_total` | counter | partition | every partition counter: `packets`, `bytes`, `messages`, `unknown_templates`, `undecodable_tail`, `schema_mismatch`, `gaps`, `dups`, `admin`, `degraded_events`, `decode_errors`, `malformed`, `truncated`, `transport_errors`, `seq_resets`, `transport_down`, `transport_up`, `recap_markers` |
| `subplane_partition_state` | gauge | partition, state | 1 for the current state (`init`, `active`, `degraded`), 0 for the others |
| `subplane_partition_last_sequence` | gauge | partition | last sequence accepted |
| `subplane_partition_last_receive_age_seconds` | gauge | partition | seconds since the last packet (0 before the first) |
| `subplane_transport_state` | gauge | partition, state | 1 for the current state (`unknown`, `up`, `down`) |
| `subplane_transport_<counter>_total` | counter | partition | the transport's own counters: `aeron` has `fragments`, `oversize`, `images_available`, `images_unavailable`, `client_errors`; `tcp_stream` has `connects`, `disconnects`, `framing_errors`; UDP has none |
| `subplane_recap_<counter>_total` | counter | partition | recap counters (architecture.md, "Recap"), without their `recap_` prefix; recap partitions only |
| `subplane_recap_inflight`, `subplane_recap_queued` | gauge | partition | recap requests in flight, and waiting |
| `subplane_group_loops_total` | counter | group | pipeline loop iterations |
| `subplane_recv_to_publish_seconds` | histogram | group | packet receive timestamp to after its last publish |
| `subplane_session_published_total`, `_filtered_total`, `_publish_failed_total` | counter | session | records published, filtered out, and not fitting the ring |
| `subplane_session_ring_bytes_total` | counter | session | bytes written to the ring (its head position) |
| `subplane_session_ring_capacity_bytes` | gauge | session | ring data capacity |
| `subplane_session_ring_heartbeat_age_seconds` | gauge | session | seconds since the writer's last heartbeat |
| `subplane_session_filter_generation` | gauge | session | generation of the current filter |
| `subplane_session_consumers` | gauge | session | processes reading the ring (Topology, "Consumers") |
| `subplane_topology_edge` | gauge | direction, role, key, partition or session | 1 when the edge is up |
| `subplane_topology_scan_unreadable` | gauge | | processes the consumer scan could not read |
| `subplane_recap_connected` | gauge | partition | 1 while the recap component is connected to its service |
| `subplane_app_lag_bytes` | gauge | session, app, pid | ring bytes the app has not read (head minus its reader's position) |
| `subplane_app_heartbeat_age_seconds` | gauge | session, app, pid | seconds since the app's reader last reported |
| `subplane_app_records_total`, `subplane_app_overruns_total` | counter | session, app, pid | records the reader returned; times it was lapped |
| `subplane_app_tracing` | gauge | session, app, pid | 1 while the reader traces read latency |
| `subplane_app_read_latency_seconds` | histogram | session, app, pid | `ingest_ts` to the app's read, traced records only; same box, same clock, so no clock sync is needed. Values from about 137 s up count in `+Inf` only |
| `subplane_reader_slots_used` | gauge | | reader slots claimed, all sessions |

**Useful queries:**
- message rate per partition: `rate(subplane_partition_messages_total[1m])`;
- p99 latency: `histogram_quantile(0.99, rate(subplane_recv_to_publish_seconds_bucket[5m]))`;
- a silent feed: `subplane_partition_last_receive_age_seconds > 5`;
- a ring that cannot keep up: `rate(subplane_session_publish_failed_total[1m]) > 0`;
- an app falling behind: `subplane_app_lag_bytes > 0.5 * on(session) group_left subplane_session_ring_capacity_bytes`;
- an app that stopped reading: `subplane_app_heartbeat_age_seconds > 5`;
- app read latency p99, while tracing: `histogram_quantile(0.99, rate(subplane_app_read_latency_seconds_bucket[5m]))`.

### Reader slots
Each app's reader reports through a slot in its ring file (architecture.md, "Reader slots"), with no app code: the SDK claims the slot on `open()` and writes its read position, heartbeat and overruns about every 64 polls. `SUBPLANE_APP_NAME` names it.

**Latency tracing** is off by default and switched by the operator:
- `POST /sessions/<name>/trace?on=1`: every reader of the session;
- `&pid=N`: one process's readers only;
- `&sample=N`: trace 1 record in N;
- `?on=0` switches it off.

A reader applies the change within about 64 polls; its `tracing` field and `subplane_app_tracing` confirm it. Readers built before slots existed, readers without write permission on the ring file (see `agent.ring_mode`), and a 65th reader on one ring read normally but do not report.

## The gateway's metrics (implemented)

`spwsgw --http 127.0.0.1:9101` serves the routes from its own thread, never the gateway thread, so `--spin` and the data path are unaffected. Without `--http` there is no server.

**Identity flags:**
- `--instance NAME` (default: the pack's `exchange`);
- `--site S`, `--env E`;
- `--labels k=v,...`, added to `subplane_service_info` only, with the agent's label-name rules.

Every sample carries `service="spwsgw"`, `instance` and `host`; the gateway's own series add `venue` (the pack's `exchange`).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `subplane_service_info` | gauge | identity, `venue` | always 1 |
| `subplane_uptime_seconds` | gauge | | seconds since start |
| `subplane_gateway_<counter>_total` | counter | venue | `frames`, `records`, `acks`, `unmatched_streams`, `unknown_symbols`, `parse_errors`, `map_errors`, `book_errors`, `published`, `offers`, `offer_bytes`, `trades`, `quotes`, `book_updates`, `book_snapshots`, `heartbeats`, `definitions`, `other`, `snapshot_requests`, `snapshots_applied`, `connects`, `disconnects`, `resyncs`, `state_messages`, `state_dropped`, `republished` |
| `subplane_gateway_dropped_total` | counter | venue, reason | messages not delivered: `back_pressured` (a subscriber is behind and the retry spin ran out), `not_connected` (nobody subscribes yet), `failed` (publication closed or at its position limit) |
| `subplane_gateway_connected` | gauge | venue | 1 while connected to the exchange |
| `subplane_gateway_publication_connected` | gauge | venue | 1 while a subscriber is connected to the output stream |
| `subplane_gateway_last_sequence` | gauge | venue | Sequence of the last message offered |
| `subplane_gateway_instruments` | gauge | venue | instruments configured |
| `subplane_gateway_instrument_state` | gauge | venue, state | instruments by SourceState: `ok`, `stale` (lost since it was ok), `unknown` (not yet ok) |
| `subplane_gateway_republish_requests_total` | counter | venue, result | Republish requests: `queued`, `not_found` (no such instrument or venue) |
| `subplane_gateway_control_<counter>_total` | counter | venue | with `--control-channel`: `frames`, `malformed`, `hellos`, `republish`, `unsupported` (answered `Status{Unsupported}`), `replies`, `reply_dropped` |
| `subplane_gateway_control_peers` | gauge | venue | requesters connected to the control input |
| `subplane_gateway_frame_to_offer_seconds` | histogram | venue | frame entry to the end of its last offer (frames that published) |
| `subplane_gateway_recv_to_offer_seconds` | histogram | venue | frame receive (`RecvTs`, `CLOCK_REALTIME`) to the end of its last offer; the agent's `recv_to_publish` starts from the same clock |
| `subplane_topology_edge` | gauge | direction, role, key, venue | 1 when the edge is up |

**Useful queries:**
- a gateway that lost its exchange: `subplane_gateway_connected == 0`;
- output nobody reads: `rate(subplane_gateway_dropped_total{reason="not_connected"}[1m]) > 0`;
- an agent that cannot keep up: `rate(subplane_gateway_dropped_total{reason="back_pressured"}[1m]) > 0`;
- books the gateway cannot recover: `subplane_gateway_instrument_state{state="stale"} > 0` for more than a minute.

## Topology

`GET /topology` returns one JSON document per service. Collectors join the documents of every service on endpoint keys, so no service needs to know any other.

```json
{ "schema": "subplane.topology/1",
  "node": { "id": "box1/subplaned/prod-box1", "service": "subplaned", "instance": "prod-box1", "host": "box1",
            "pid": 4242, "version": "...", "epoch": 1792..., "site": "ny4", "env": "prod" },
  "upstream": [
    { "key": "aeron-ipc://box1/dev/shm/aeron-feeds#1001", "role": "data", "state": "up",
      "local": { "partition": "binance" }, "describe": "aeron aeron:ipc stream 1001 connected",
      "publisher": { "aeron_session": 1234 } },
    { "key": "aeron-udp://snap-host:40500#1101", "role": "recap", "state": "up", "local": { "partition": "binance" } } ],
  "downstream": [
    { "key": "shm-ring://box1/dev/shm/subplane.prod-box1.mm.ring", "role": "data", "state": "up",
      "local": { "session": "mm", "owner": "config", "partitions": ["binance"] },
      "consumers": [ { "pid": 777, "start_time": 6619238, "uid": 1000, "comm": "mm_engine", "name": "market-maker",
                       "unit": "mm-engine.service", "via": ["control", "mapping", "slot"],
                       "reader": { "slot": 0, "lag_bytes": 0, "heartbeat_age_seconds": 0.004, "records": 81234, "overruns": 0,
                                   "tracing": false, "trace_requested": false, "sdk_version": 1, "capabilities": 1 } } ] } ],
  "scan": { "processes": 690, "unreadable": 12 } }
```

### Fields
- **`node.id`:** `<host>/<service>/<instance>`. `host` is written as `gethostname()` returns it, the same as the `host` label in `/metrics`.
- **`role`:** what is lost if the edge breaks:
  - `data`: the live stream;
  - `recap`: recovery only;
  - `control`: a request path, for example a gateway's republish input (planned).
- **`state`:**
  - for a `data` edge, the transport state: `up`, `down` or `unknown`;
  - for a `recap` edge, whether the recap component is connected to its snapshot service;
  - a ring is `up` while the agent runs.
- **`publisher`:** what the transport knows about the current source. For Aeron this is the newest image's session id; a new id means a new publisher instance.

### Consumers
An app needs no code to appear in `consumers`. The agent finds it in three ways:
- **`control`:** the app's connection created or attached to the session on the control socket. The agent knows its pid and uid (`SO_PEERCRED`) and, if the app sent `IDENTIFY`, its name.
  - The SDK sends `IDENTIFY` on connect when `SUBPLANE_APP_NAME` is set.
  - `ControlClient::identify(name)` does the same in code.
- **`slot`:** the app's reader claimed a reader slot in the ring (Metrics, "Reader slots"). The entry then carries a `reader` object: `slot`, `lag_bytes`, `heartbeat_age_seconds`, `records`, `overruns`, `tracing`, `trace_requested`, `sdk_version`, `capabilities`. A slot names its process even when the `/proc` scan cannot read it.
- **`mapping`:** the process maps the session's ring file, according to a scan of `/proc/<pid>/maps`. This catches apps that only read the ring.
  - The scan is cached for 2 s.
  - It cannot read other users' processes unless the agent runs as root or with `CAP_SYS_PTRACE`. Those processes are counted in `scan.unreadable`.
  - An agent in its own container sees only its pid namespace.

**Matching and naming:**
- A process found several ways is one entry, for example `"via": ["control", "mapping", "slot"]`.
- A process is identified by (`pid`, `start_time`), so a reused pid is not mistaken for the old process.
- `name` is the `IDENTIFY` name if one was sent, else the slot's name (`SUBPLANE_APP_NAME`), else the systemd unit or container scope (`unit`, from `/proc/<pid>/cgroup`), else `comm`.
- A process still mapping a ring that an agent restart unlinked has `"stale_ring": true` and is not counted in `subplane_session_consumers`.

### The gateway's topology
- **Upstream `data`:** the pack's WebSocket URL, as a URL key. `state` is `up` while connected. On `--replay`, the recording, as `file://<path>`, always `up`.
- **Upstream `recap`:** each distinct REST book-snapshot endpoint, live only. It is the exchange's recovery path: losing it loses book resyncs, not the live stream. A REST endpoint has no connection to watch, so it is reported `up`; failures show in `snapshot_requests` against `snapshots_applied`.
- **Downstream `data`:** the Aeron publication, keyed as the agent keys its upstream (the driver directory resolves the same way). `state` is `up` while a subscriber is connected. An Aeron publication knows only whether someone subscribes, not who; the collector finds who by joining on the key. `--out stdout` and `--out null` have no downstream edge.
- **Upstream `recap`, control:** with `--control-channel`, the SPP request stream (Republish). `state` is `up` while a requester is connected.
- **Downstream `recap`:** the SourceState stream (`--state-stream`, SPP), keyed the same way. Losing it loses recovery, not data. `state` is `up` while a subscriber is connected.
- `local` names the `venue`, `venue_id` and number of `instruments`.

### Endpoint keys
A key names one stream. One service's downstream key equals the next service's upstream key. These rules are normative, and both repositories check them against the vectors in `docs/topology-keys.json`:

| Endpoint | Key |
|---|---|
| Aeron IPC | `aeron-ipc://<host><aeron_dir>#<stream>`. IPC is per media driver, so the driver directory is part of the key: the configured `aeron_dir`, or Aeron's default (`/dev/shm/aeron-<user>`). |
| Aeron UDP | `aeron-udp://<endpoint>#<stream>`, from the channel's `endpoint=`, else `control=`. Every other parameter is dropped. |
| UDP | `udp-mcast://<group>:<port>`, or `udp://<address>:<port>` for unicast |
| TCP | `tcp://<address>:<port>` |
| Ring | `shm-ring://<host><path>` |
| URL (exchange WebSocket, REST) | `wss://…`, `https://…`: scheme and host lower-cased; default port, user info, query and fragment dropped |

**Hosts in keys:**
- The local-host spellings name this box and are replaced by the host name: `""`, `*`, `0.0.0.0`, `::`, `localhost`, `127.0.0.1`, `::1`.
- Other addresses are lower-cased and kept as written; there is no DNS lookup.
- For keys to join across boxes, configure endpoints by host name consistently on both ends. A mismatch shows up as a dangling edge, not a silent error.

### Topology as metrics
`/metrics` carries the same edges, so PromQL and alerts work without a collector:
- **`subplane_topology_edge{direction, role, key, partition|session}`:** 1 when up, 0 when not.
- **`subplane_session_consumers{session}`:** the processes reading each ring.
- **`subplane_topology_scan_unreadable`:** the processes whose maps the consumer scan could not read.

Example alert, a session nobody reads: `subplane_session_consumers == 0`.

### Planned
- **Collector:** `subplane topo fetch | render | impact | serve`.
  - It joins documents into a flow graph and renders it as Mermaid, dot or a Grafana node graph.
  - It answers "if X fails, what loses data and what loses only recovery".
