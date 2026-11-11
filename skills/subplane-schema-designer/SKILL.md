---
name: subplane-schema-designer
description: Design or review SBE message schemas that fit subplane - a bus schema for a publisher or gateway, a new message or field in an existing schema, an SPP (subplane protocol) core message, or an SPP extension for app-specific control. Use when someone asks to design, add, change or review a market-data message, a schema XML, a control/request message, or an extension for subplane, or asks whether a schema change is compatible.
---

# subplane schema designer

You help people design messages that subplane routes cheaply, that evolve without breaking deployed peers, and that follow its philosophy: the agent decodes routing fields only, apps decode the rest with their own codecs, and data flows one way while control travels in SPP.

Read the reference that matches the task before you write XML:
- `references/philosophy.md`: the rules for data messages (bus schemas).
- `references/evolution.md`: how a schema may change, and what needs a new schemaId.
- `references/spp-extensions.md`: SPP core messages and extensions.
- `references/examples.md`: annotated canonical messages and a worked extension.

## Workflow

1. **Classify each message.** Ask if it is unclear.
   - **Live data:** trade, quote, book, status, definition. It travels on the bus and the agent routes it.
   - **Bus admin:** heartbeat, sequence reset, trading status. Same bus, routed by topic.
   - **Recap reply marker:** only for a firm bus whose snapshot service already speaks the bus schema. New services use SPP's `RecapBegin`/`RecapRecord`/`RecapEnd` instead.
   - **SPP core message:** a control conversation between subplane services. This is rare: it changes the protocol, so it needs a spec change.
   - **SPP extension:** app-specific control, carried in the `Extension` envelope with its own body schema.
2. **Gather the facts.**
   - Who sends it, and who reads it.
   - How often: per tick, or rarely.
   - Which fields routing needs: instrument, sequence, instrument sequence (optional, per instrument across the topic), exchange time, venue.
   - Whether the value is self-contained (last value wins) or built up from many messages (a book, accumulated volume). Built-up state needs recap, not the last-value cache.
   - For a change to an existing schema: the current XML.
3. **Draft the XML** with the rules in the matching reference:
   - routing fields in one shared prefix;
   - flat and naturally aligned for hot data;
   - int64 fixed point for prices;
   - integer instrument ids;
   - `CorrelationId` first in control messages;
   - `sinceVersion` on anything appended.
4. **Check it with the tool.** It must report no errors:
   ```
   subplane schema --schema new.xml --lint --routing InstrumentId,VenueId,Sequence,ExchangeTs --admin 100   # bus schema
   subplane schema --schema new.xml --lint --control                                                       # control or extension body
   subplane schema --schema new.xml --lint --against old.xml ...                                            # any change to a published schema
   ```
   `--strict` turns warnings into a failure, and `--views ... --message A,B` limits generation to some messages. Explain every warning you keep. Then generate what the users will compile:
   - `--views cpp|numpy|q` for flat data messages;
   - `--codec cpp` for control messages and extension bodies.
5. **Write the compatibility note:**
   - the `docs/compat-log.md` row: audience, impact (additive, behaviour or breaking), migration;
   - any upgrade-order constraint. The normal order is agents, then gateways with their snapshot services, then apps. A change that only works in another order must say so.
   - For a new canonical template, the agent config mapping that must ship before any gateway sends it.
6. **Show the result:** the XML, the lint output, the generated-code entry points, the compat row, and a short "why" for each non-obvious choice.

## Things to refuse or push back on
- **Asking the agent to decode more than routing fields** (filtering on price, computing values). That belongs in the app or a derived feed.
- **Strings or floats in hot data messages.** Use ids and fixed point, with names in definition messages.
- **Changing, moving or removing a field in a published schema.** It can only be appended; anything else needs a new schemaId and a migration plan.
- **App-to-publisher data paths.** Apps never publish through the agent. Control to a declared service is fine, as an extension.
- **An extension that duplicates something SPP has** (recap, republish). Use the core message.
- **Control that changes what other apps or boxes receive** (pause an instrument, change a publisher's behaviour). If one app only wants to stop *receiving* something, that is its own session filter, with no new message. If it really is a shared action, design it as operator-authorised, with identity in the body (the agent never reads it), a reason, and an automatic expiry. Say who may send it.
