# Examples

## A canonical data message (from schemas/subplane_canonical.xml)

```xml
<sbe:message name="Trade" id="1" blockLength="72" description="One trade">
  <!-- shared 40-byte prefix: routing fields at fixed offsets in every data message -->
  <field name="InstrumentId" id="1" type="uint32" offset="0"/>   <!-- integer id; names come in InstrumentDefinition -->
  <field name="VenueId"      id="2" type="uint16" offset="4"/>
  <field name="Flags"        id="3" type="uint16" offset="6"/>
  <field name="Sequence"     id="4" type="uint64" offset="8"/>   <!-- one counter per stream -->
  <field name="InstrumentSeq" id="8" type="uint64" offset="16"/> <!-- one counter per instrument across the topic, 0 = not counted -->
  <field name="ExchangeTs"   id="5" type="Ts"     offset="24"/>
  <field name="RecvTs"       id="6" type="Ts"     offset="32"/>
  <!-- message body: fixed point, naturally aligned (payload offset = 8 + root offset) -->
  <field name="Price"        id="10" type="Price" offset="40"/>  <!-- int64 x 1e-9 -->
  ...
</sbe:message>
```
Lint: `--routing InstrumentId,VenueId,Sequence,ExchangeTs --admin 100` reports no errors and no warnings.

## Adding a field later (evolution)
```xml
<field name="MatchTs" id="20" type="Ts" offset="72" sinceVersion="2" description="venue matching-engine time, 0 if not sent"/>
```
- `blockLength` grows to 80 and the schema `version` becomes 2.
- Old apps still read 72 bytes; new apps see `MatchTs = 0` from old gateways (read them with `read<T>()`, not `view<T>`).
- Check it with `--lint --against` the published file.

## Mapping a new template in the agent (before any gateway sends it)
```yaml
topics:
  - name: crypto.binance-spot
    decoder:
      type: sbe_routing
      schema: schemas/subplane_canonical.xml
      fields: { symbol: InstrumentId, sequence: Sequence, instrument_sequence: InstrumentSeq, exchange_ts: ExchangeTs, venue: VenueId }
      messages:
        - { template: 3,  msg_type: BOOK_UPDATE }
        - { template: 11, msg_type: STATUS }      # the new template; data types are TRADE, QUOTE, BOOK_UPDATE, BOOK_SNAPSHOT, OHLCV, STATUS, DEFINITION
        - { template: 100, admin: HEARTBEAT }
```

## Boilerplate for an extension body schema
```xml
<?xml version="1.0" encoding="UTF-8"?>
<sbe:messageSchema xmlns:sbe="http://fixprotocol.io/2016/sbe"
                   package="acme.gwstatus" id="7001" version="1"
                   byteOrder="littleEndian" headerType="messageHeader">
  <types>
    <composite name="messageHeader">
      <type name="blockLength" primitiveType="uint16"/>
      <type name="templateId" primitiveType="uint16"/>
      <type name="schemaId" primitiveType="uint16"/>
      <type name="version" primitiveType="uint16"/>
    </composite>
    <!-- only if a message has a repeating group -->
    <composite name="groupSizeEncoding">
      <type name="blockLength" primitiveType="uint16"/>
      <type name="numInGroup" primitiveType="uint16"/>
    </composite>
    <!-- only if a message has variable-length bytes or text -->
    <composite name="varData32">
      <type name="length" primitiveType="uint32"/>
      <type name="varData" primitiveType="uint8" length="0"/>
    </composite>
    <enum name="BookState" encodingType="uint8">
      <validValue name="Unknown">0</validValue>
      <validValue name="Syncing">1</validValue>
      <validValue name="InSync">2</validValue>
    </enum>
  </types>
  <!-- messages go here; every root starts with CorrelationId (uint64, offset 0) -->
</sbe:messageSchema>
```

## A worked extension: "instrument status from the gateway"
Need: an app wants to ask its gateway whether an instrument's book is in sync, without waiting for the next live message.

1. **Id:** 0x8001, private to the deployment.
2. **Body schema** `acme_gwstatus.xml`, schemaId 7001, version 1:
   ```xml
   <sbe:message name="StatusQuery" id="1" blockLength="16">
     <field name="CorrelationId" id="1" type="uint64" offset="0"/>
     <field name="InstrumentId"  id="2" type="uint32" offset="8"/>
     <field name="VenueId"       id="3" type="uint16" offset="12"/>
   </sbe:message>
   <sbe:message name="StatusAnswer" id="2" blockLength="32">
     <field name="CorrelationId" id="1" type="uint64" offset="0"/>
     <field name="LastSequence"  id="2" type="uint64" offset="8"/>
     <field name="LastResyncTs"  id="3" type="uint64" offset="16"/>
     <field name="InstrumentId"  id="4" type="uint32" offset="24"/>
     <field name="BookState"     id="5" type="BookState" offset="28"/>  <!-- enum uint8: Unknown, Syncing, InSync -->
   </sbe:message>
   ```
3. **Envelope:** `Extension{ExtensionId=0x8001, ExtType=1 (StatusQuery), Flags=reply-expected}`. The answer is `Extension{0x8001, ExtType=2}` with the same `CorrelationId`.
4. **Advertising:** the gateway's `Hello` lists `{0x8001, 1}`. The app gets `Unsupported` from gateways that don't.
5. **Checks:**
   - `subplane schema --schema acme_gwstatus.xml --lint --control` reports no errors;
   - `--codec cpp --namespace acme_gwstatus` generates the codec.
6. **Compat row:** additive, apps and gateway, no upgrade-order constraint (old gateways answer `Unsupported`).

Why not a core message? Only this deployment's apps need it, and a gateway can answer it without the agent understanding anything.
