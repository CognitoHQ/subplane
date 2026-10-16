# Changing a published schema

A published schema is read by peers you do not upgrade at the same time. Agents are upgraded first, then gateways together with their snapshot services, then apps, and each works with peers one release back. So every change must be readable by the older side, in both directions.

## Allowed without a new schemaId
- **Append a field** at the end of a root block or group entry, with `sinceVersion="N"` where N is the new schema version. Older readers stop at their `blockLength`; newer readers see the field as absent (0) in older senders' messages.
- **Append a group or data element** after the existing ones, with `sinceVersion`. A group cannot follow existing data: groups precede data in SBE, so that is not appendable.
- **Add a message** with a new template id.
- **Add enum values.** Readers must treat unknown values as "unknown", never as an error.
- **Bump `version`** on every one of these changes. The lint refuses a changed schema with the same version.
- **Constants:** a field appended before a trailing `presence="constant"` field (canonical messages end with `PriceExponent`) is fine. Constants occupy no bytes.
- **`sinceVersion` is required** on everything appended; the lint treats a missing one as an error.

## Needs a new schemaId (a new schema, with a migration plan)
- Removing, moving, retyping or resizing a field.
- Reordering groups or data.
- Changing what a field means, its units or its exponent.
- Changing the header.

## Checking it
```
subplane schema --schema new.xml --lint --against published.xml
```
It reports removals, moves, retypes, fields added inside the old block, missing `sinceVersion`, a group added after data, a shrinking block, and a missing version bump.

## Rolling it out
- **A new data template:** map it in the agent config first. Agents are upgraded first, and an unmapped template is counted and dropped, never misrouted. Then upgrade the publisher.
- **A new field:** publishers can send it right away, because old readers skip it. Apps use it once they are rebuilt with the new views or codec.
- **New apps reading older senders** (a gateway rolled back, or a mixed fleet) see shorter blocks:
  - the SPP-style codec (`--codec cpp`) reads absent fields as 0;
  - with generated views, use `read<T>(payload, len)` (C++, a copy, zero-filled) or `message_from_bytes` (Python). The zero-copy `view<T>` refuses a block shorter than the struct.

  Tell app developers which one to use.
- **Record it** in `docs/compat-log.md`: audience, impact, migration, and any upgrade-order constraint.
