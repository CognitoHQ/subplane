# Observability contract

Every subplane service serves the same observability surface on its HTTP port, so one Prometheus, one Grafana and one topology collector cover the agent (`subplaned`), the gateways (`spwsgw`) and the services still to come (`spwsgw-snap`, the relay). This document is the contract. The `subplane-feeds` repository keeps an identical copy.

**Status:**
- The agent's `/metrics` is implemented.
- `/topology`, the gateway's HTTP server and the collector are planned.

## Routes

| Route | Returns | Agent | Gateway |
|---|---|---|---|
| `GET /metrics` | Prometheus text exposition 0.0.4 (`text/plain; version=0.0.4`) | implemented | planned |
| `GET /metrics?format=json` | the same counters as JSON, for humans and scripts | implemented | planned |
| `GET /topology` | what the service consumes and what it feeds, `"schema": "subplane.topology/1"` | planned | planned |
| `GET /health` | 200, or 503 with a reason | implemented (always 200) | planned |

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

**Useful queries:**
- message rate per partition: `rate(subplane_partition_messages_total[1m])`;
- p99 latency: `histogram_quantile(0.99, rate(subplane_recv_to_publish_seconds_bucket[5m]))`;
- a silent feed: `subplane_partition_last_receive_age_seconds > 5`;
- a ring that cannot keep up: `rate(subplane_session_publish_failed_total[1m]) > 0`.

## Topology (planned)
`/topology` names each upstream and downstream endpoint by a canonical key, for example `aeron-ipc://<host><aeron_dir>#<stream>` or `shm-ring://<host><path>`. Each entry carries a `role` (`data`, `recap`, `control`) and a state. A collector joins the documents of every service on those keys into a live flow graph, and answers "if X fails, what downstream loses data, and what loses only recovery".

The full design is planned in observability PR B (the agent's `/topology`, key normalization with shared test vectors in `docs/topology-keys.json`, and consumer apps found through the control socket and `/proc`) and in the collector PR (`subplane topo fetch | render | impact | serve`).
