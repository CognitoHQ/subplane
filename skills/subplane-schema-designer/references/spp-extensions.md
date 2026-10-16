# SPP core messages and extensions

The spec is `docs/protocol/subplane-protocol.md` in the subplane repo, and the schema is `schemas/subplane_proto.xml` (schemaId 101).

## When to use what
| Need | Use |
|---|---|
| Recap, the state of an instrument as of a sequence | core: `RecapRequest`, then `RecapBegin`, `RecapRecord`…, `RecapEnd` or `RecapReject` |
| Ask a publisher to publish again | core: `Republish`, answered by `Status` |
| Something only your apps and your service understand | an extension |
| Something every subplane deployment needs | a new core message: a spec change, discussed first |

## Control message rules (core messages and extension bodies)
- **`CorrelationId`** (u64) starts every root block. A request sets it unique among the sender's outstanding requests; every reply echoes it.
- **Root blocks are naturally aligned,** group entries are fixed blocks, and varData comes after the groups. Groups and data are fine here; these messages are not on the hot path.
- **Every message gets an answer or is fire-and-forget,** never "maybe". A request expecting a reply gets one, or a `Status`.
- **Unknown is not an error.** Answer `Status{Unsupported}`, and never close the connection over it.
- **Size:** keep bodies small (well under 64 KiB). Big payloads belong on a data path.
- Lint with `--control`.

## Designing an extension
1. **Pick an id.** Use 0x8000-0xFFFF for your own deployment, with no registration. For one that is general, propose an id below 0x8000 in `docs/protocol/extensions.md`.
2. **Give the body its own SBE schema** with its own schemaId: your own id space, outside 100 and 101. List the operations as messages (`ExtType` in the envelope matches their template ids).
3. **Advertise it:** the service lists `{ExtensionId, Version}` in its `Hello`. Bump the extension's version when you add an operation, and use a new operation only with peers that advertise it.
4. **Set the flags:**
   - must-understand (bit 0) when ignoring the message would be wrong;
   - reply expected (bit 1) when the sender waits.
5. **Evolve the body** with the same append-only rules (`evolution.md`).

## Through the agent (planned)
Apps send extensions over the agent's control socket. The agent forwards them only:
- to a service declared in its config,
- for an id the config allows and the service advertised in its `Hello`,

and it relays the reply. The agent never reads the body, so put everything the service needs in the body, including any identity your service checks.
