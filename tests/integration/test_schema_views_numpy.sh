#!/usr/bin/env bash
# sp_schema --views numpy, loaded by python3 + numpy: a Trade built with the dtype
# round-trips through bytes, RECORD_HEADER is 64 bytes. Skips (77) without numpy.
# usage: test_schema_views_numpy.sh <sp_schema> <schema.xml> <canonical.xml>
set -u
TOOL=$1; SCHEMA=$2; CANON=$3
command -v python3 >/dev/null 2>&1 || { echo "SKIP: python3 not available"; exit 77; }
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
fail() { echo "FAIL: $*"; exit 1; }
"$TOOL" --schema "$SCHEMA" --views numpy --out "$TMP" 2> "$TMP/report.txt" || fail "sp_schema: $(cat "$TMP/report.txt")"
"$TOOL" --schema "$CANON" --views numpy --namespace sp_canonical --out "$TMP" 2>> "$TMP/report.txt" || fail "sp_schema canonical: $(cat "$TMP/report.txt")"
grep -q "skipped BookUpdate: has groups" "$TMP/report.txt" || fail "report lacks the skipped message: $(cat "$TMP/report.txt")"
cat > "$TMP/check.py" <<'PY'
import struct, sys
sys.path.insert(0, sys.argv[1])
import sample_md_views as v
# Dataclasses: standard library only.
assert v.Trade.SIZE == 47 and v.Quote.SIZE == 64 and v.RecordHeader.SIZE == 64
assert v.TEMPLATE_ID['Quote'] == 2 and v.BY_TEMPLATE[1] is v.Trade
raw = struct.pack(v.Trade.FORMAT, 39, 1, v.SCHEMA_ID, v.SCHEMA_VERSION, 424242, 77, 1758742200000000000, 3, -12345, 500, 2)
assert len(raw) == 47 and raw[0:2] == b'\x27\x00' and int.from_bytes(raw[34:42], 'little', signed=True) == -12345
t = v.Trade.from_bytes(raw)
assert (t.SecurityID, t.SeqNum, t.MarketSegmentID, t.Price, t.Qty, t.Side) == (424242, 77, 3, -12345, 500, 2), t
assert v.message_from_bytes(raw) == t
assert v.message_from_bytes(raw[:-1]) is None            # short
assert v.message_from_bytes(b'\x27\x00\x63\x00' + raw[4:]) is None   # template 99: not in the module
tick = struct.pack(v.Ticker.FORMAT, 24, 4, v.SCHEMA_ID, 2, b'ESZ6\0\0\0\0', 5, 6)
assert v.Ticker.from_bytes(tick).Symbol == b'ESZ6\0\0\0\0'
rec = struct.pack(v.RecordHeader.FORMAT, 4096, 77, 1758742200000000000, 1758742200000000001, 424242, 47, 1, v.SCHEMA_ID, 2, 1, 0, 1, 0, 30, 0) + raw
hdr, msg = v.parse(rec)
assert hdr.symbol_id == 424242 and hdr.payload_len == 47 and msg == t, (hdr, msg)
hdr2, msg2 = v.parse(rec[:51] + b'\x01' + rec[52:])    # kind 1 (control) at byte 51: no message
assert hdr2.kind == 1 and msg2 is None
import subplane_canonical_views as c
snap = struct.pack(c.BookSnapshot.FORMAT, 360, 4, c.SCHEMA_ID, 1, 7, 3, 0, 55, 0, 0, *range(100, 110), *range(10), *range(200, 210), *range(10), 10)
bs = c.BookSnapshot.from_bytes(snap)
assert len(snap) == 368 and bs.InstrumentId == 7 and bs.Depth == 10 and bs.BidPx == tuple(range(100, 110)) and bs.AskPx[9] == 209, bs
tr = c.Trade.from_bytes(struct.pack(c.Trade.FORMAT, 64, 1, c.SCHEMA_ID, 1, 7, 3, 0, 1001, 0, 0, 65432100000000, 250000000, 99, 2))
assert tr.Price == 65432100000000 and tr.Aggressor == 2 and c.message_from_bytes(snap) == bs
print("dataclass views ok")
# NumPy, when present.
try:
    import numpy as np
except ImportError:
    print("numpy absent: dtype part skipped"); sys.exit(0)
assert v.RECORD_HEADER.itemsize == 64 and v.Trade.DTYPE.itemsize == 47 and v.Quote.DTYPE.itemsize == 64
back = np.frombuffer(raw, dtype=v.Trade.DTYPE)[0]
assert back['SecurityID'] == 424242 and back['Price'] == -12345 and back['Qty'] == 500 and back['Side'] == 2
col = np.frombuffer(raw * 3, dtype=v.Trade.DTYPE)
assert col.shape == (3,) and int(col['Qty'].sum()) == 1500
h = np.frombuffer(rec, dtype=v.RECORD_HEADER, count=1)[0]
assert h['payload_len'] == 47 and h['template_id'] == 1
assert c.BookSnapshot.DTYPE['BidPx'].shape == (10,) and c.BookSnapshot.DTYPE.itemsize == 368
arr = np.frombuffer(snap, dtype=c.BookSnapshot.DTYPE)[0]
assert list(arr['BidPx']) == list(range(100, 110)) and arr['Depth'] == 10
print("numpy views ok")
PY
python3 -I "$TMP/check.py" "$TMP" || fail "python check failed"
echo "PASS"
