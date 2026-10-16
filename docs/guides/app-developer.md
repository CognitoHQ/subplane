# App developer guide

You write an application that consumes market data on a box where `subplaned` runs. This guide is all you need. The internals live in `docs/context/architecture.md`; you should not have to read it.

**Your view of subplane:**
- A **session** is your subscription: which partitions, symbols and message types you want.
- A **ring** is the shared-memory file the agent writes your session's records to. You read it and the agent never waits for you.
- Each **record** is a 64-byte header (symbol, sequence, timestamps, type) followed by the publisher's bytes, unchanged. You decode them with your own codecs or with generated views.

You never see the bus transport (multicast, Aeron, TCP), the publisher's protocol, or other apps.

## 1. Get a session

There are two ways:
- **In the agent's config:** your operator adds a `sessions:` entry. The ring is created at agent start and lives as long as the agent does.
- **From your code:** `ControlClient::create_session` (or `attach` to an existing one) over the agent's control socket. A session you create lives while you are connected, plus a short grace period (`control.linger_ms`) after you disconnect.

```cpp
#include <subplane/client/control_client.h>
#include <subplane/client/shm_reader.h>
using namespace subplane;

ControlClient cc;
if (!cc.connect("/run/subplane/agent1.sock")) { /* the agent is not running, or another socket path */ }
SessionRequest req;
req.name = "mm";
req.symbols = {"BTCUSDT", "ETHUSDT"};
AttachInfo info;
cc.create_session(req, info);   // or cc.attach("mm", info)
```

## 2. Read the ring

```cpp
ShmReader rd;
std::string err;
rd.open(info.ring_path, &err);
rd.seek_to_head();                     // live tail; keep position 0 to read what is still in the ring
RecordView rv;
for (;;) {
    switch (rd.poll(rv)) {
    case ReaderStatus::Ok:
        if (is_data(*rv.hdr)) decode(rv.hdr->symbol_id, rv.payload, rv.payload_len);
        if (!rv.valid()) discard_last_decode();   // the writer lapped you while you decoded
        break;
    case ReaderStatus::Empty:   break;             // caught up
    case ReaderStatus::Overrun: resnapshot(); break; // you fell a whole ring behind: data lost
    case ReaderStatus::Closed:  reattach(); break;   // agent restarted or session gone
    default: break;
    }
}
```

Rules that keep you correct:
- `poll()` never blocks, allocates or throws. Spin or sleep between polls as your latency needs dictate.
- The record points into shared memory. Decode it, then call `rv.valid()`; if it is false, throw away what you decoded.
- `Overrun` means you lost data: rebuild your state (step 4).
- Skip control codes and message types you do not know.

Full API: `include/subplane/client/shm_reader.h` and `control_client.h` (header-only, libc and the standard library only).

## 3. Decode payloads

The payload is the publisher's message, byte for byte. Either:
- use your own SBE codecs, as you would on the bus; or
- generate typed views: `subplane schema --schema <schema.xml> --views all --out <dir>` writes a C++ header, a NumPy module and a q script for every flat message (README, "Typed views without a codec").

## 4. Start from a known state

When you join late, restart, or lose data, you need current state before applying live updates:
- **Self-contained values** (last trade, last quote, instrument definition): `ControlClient::snapshot()` returns the last value of each symbol and type your filter accepts, and the ring position to resume from. `examples/shm_demo_app.cpp` shows the late-join idiom.
- **Built-up state** (order books, accumulated volume): ask for a **recap session**: `recap: true` in YAML, or `SessionRequest::recap = true`. A recap session needs a symbol list and agent-control events. The agent fetches a snapshot from the publisher's snapshot service and merges it into your live stream with no gap and no duplicate. For each symbol you see:

  ```
  RECAP_PENDING{symbol}
  RECAP_BEGIN{as_of_seq = S, record_count}
  snapshot records (FLAG_SNAPSHOT_ORIGIN set, sequence = S)
  RECAP_COMPLETE{S, records on this ring}
  live records, sequence > S, no gap, no duplicate
  ```

  - Build your state from the snapshot records, then apply live records.
  - On `RECAP_FAILED{status, attempt}` the symbol stays pending and the agent asks again. Keep it marked stale until `RECAP_COMPLETE`.
  - On `SYMBOL_STALE{reason}` (a sequence gap, a sequence reset or a publisher restart) your state for the symbol is no longer exact. Mark it stale and wait: the agent has already started a new recap, and the same sequence follows. Throw your state away at `RECAP_BEGIN`, not before, so you can keep showing it as stale.
  - Other sessions on the box are never delayed by your recap.

## 5. Name your app

Set `SUBPLANE_APP_NAME=market-maker` in your app's environment. The agent then shows your app by that name to operators (`/topology`, metrics, dashboards). Without it, they see your systemd unit or process name.

You do not need code for operators to see your app. On `open()`, the SDK claims a reader slot in the ring and reports your read position, heartbeat and overruns through it, so operators can see your lag and whether you are still reading. When an operator switches on latency tracing, the SDK also records how long records took to reach you; this costs one clock read per sampled record, and nothing when tracing is off. To opt out, set `ReaderOptions::report = false` when you open the ring.

## 6. Upgrades

- **The agent is upgraded independently of your app.** An app built against an older SDK keeps working against a newer agent. You never have to rebuild because the agent changed.
- **New features arrive when you rebuild.** A feature that needs SDK code (for example reporting through reader slots) starts working the next time you build against the newer SDK, on your schedule.
- **Code defensively:** skip control codes and message types you do not know, read `payload_len` instead of assuming a size, and treat an `UNSUPPORTED` reply as "this agent does not offer that yet".

## 7. Ask a service something (planned)

`ControlClient::relay(service, extension_id, ext_type, body, reply_cb, timeout)` will forward a request to a service declared in the agent's config, if the id is allowed. You get the reply, or `Unsupported`, `Denied` or `Timeout`, quickly. The agent never reads your body.

## When something looks wrong

| Symptom | Likely cause | What to do |
|---|---|---|
| `open()` fails with `LayoutMismatch` | not a subplane ring, or a ring from an incompatible major version | check the path; ask your operator which agent version runs |
| frequent `Overrun` | your app is too slow for the ring's size | read faster, or ask for a larger ring (`ring.capacity_mb`) |
| `Closed`, or `writer_alive()` false | the agent restarted or your session was destroyed | reattach and rebuild state |
| `GAP_DETECTED` or `SYMBOL_STALE` control records | the bus lost messages upstream | rebuild that symbol's state (snapshot or recap) |
| operators cannot see your app's lag | your app was built with an older SDK, or the ring file is not writable by your user | rebuild; ask your operator about `agent.ring_mode` |
