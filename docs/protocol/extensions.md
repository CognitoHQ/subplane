# SPP extension registry

Extension ids carried in the `Extension` envelope (template 900) and advertised in `Hello` (`docs/protocol/subplane-protocol.md`, section 7).

| Range | Owner |
|---|---|
| 0x0000 | invalid |
| 0x0001-0x7FFF | assigned by subplane, listed below |
| 0x8000-0xFFFF | private to a deployment; never assigned here |

## Assigned

None yet.

## Adding one
1. Open a pull request that adds a row here: id, name, owner, a one-line purpose, and the schema of its body (link it).
2. Design the body with the same rules as SPP: append-only, a `CorrelationId` echo when a reply is expected, a version advertised in `Hello`.
3. Add a `docs/compat-log.md` entry.
