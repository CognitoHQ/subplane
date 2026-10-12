#!/usr/bin/env bash
# Fetch public CME MDP 3.0 sample packets and schemas for the real-bus check.
#
# Nothing here is redistributed with flashtick (the source repositories carry
# no licence, the schema is CME's), so this script downloads at use time:
#   - 4 raw MDP 3.0 UDP payloads (schema v9 on the wire) from dkumsh/sbe_gen
#   - 5 small captures (schema v6 on the wire) from p4fpga/p4fpga-examples
#   - 1 Databento sample capture (18 heartbeats, 2 channels) from
#     AdityaTanejaRox/cme_mdp3_dashboard
#   - templates_FixBinary.xml v9 (epam/java-cme-mdp3-handler) and v13
#     (dkumsh/sbe_gen)
# and merges every UDP packet into one pcap, cme_all.pcap (27 packets,
# 31 messages, templates 12 30 32 35 47 48 51). The capture mixes channels, so
# sequence gaps and backward jumps are expected.
#
# usage: scripts/fetch-cme-sample.sh [dest-dir]      (default: build/cme-sample)
set -euo pipefail
DEST=${1:-build/cme-sample}
mkdir -p "$DEST/raw"
RAW=https://raw.githubusercontent.com
get() { curl -fsSL --retry 3 -o "$DEST/raw/$2" "$1" || { echo "fetch failed: $1" >&2; exit 1; }; }

for t in 30 47 48 51; do
    get "$RAW/dkumsh/sbe_gen/HEAD/examples/cme_mdp3_pcap_dump/tests/fixtures/template_$t.bin" "template_$t.bin"
done
for f in mdp-256 mdp-512 mdp-530 mdp-1024 mid-packet; do
    get "$RAW/p4fpga/p4fpga-examples/HEAD/07-mdp/$f.pcap" "p4_$f.pcap"
done
get "$RAW/AdityaTanejaRox/cme_mdp3_dashboard/HEAD/data/dc3-glbx-ab-dedup-20230716T080000.pcap" "databento.pcap"
get "$RAW/epam/java-cme-mdp3-handler/master/mbp-only/src/test/resources/templates_FixBinary.xml" "templates_v9.xml"
get "$RAW/dkumsh/sbe_gen/HEAD/examples/cme_mdp3_pcap_dump/schemas/cme_mdp3/templates_FixBinary.xml" "templates_v13.xml"
cp "$DEST/raw/templates_v9.xml" "$DEST/raw/templates_v13.xml" "$DEST/"

python3 -I - "$DEST" <<'PY'
import struct, sys, glob, os
dest = sys.argv[1]; raw = os.path.join(dest, 'raw')
def pcap_frames(fn):
    d = open(fn, 'rb').read(); magic = struct.unpack_from('<I', d, 0)[0]
    assert magic in (0xa1b2c3d4, 0xa1b23c4d), fn
    off = 24; out = []
    while off + 16 <= len(d):
        _, _, cl, _ = struct.unpack_from('<IIII', d, off); off += 16
        out.append(d[off:off + cl]); off += cl
    return out
def udp_frame(payload):
    udp = struct.pack('>HHHH', 14310, 14310, 8 + len(payload), 0) + payload
    ip = struct.pack('>BBHHHBBH4s4s', 0x45, 0, 20 + len(udp), 0, 0x4000, 64, 17, 0,
                     bytes([205, 209, 221, 70]), bytes([224, 0, 31, 2])) + udp
    return b'\x01\x00\x5e\x00\x1f\x02' + b'\x00\x11\x22\x33\x44\x55' + b'\x08\x00' + ip
frames = []
for fn in ['databento.pcap', 'p4_mdp-256.pcap', 'p4_mdp-512.pcap', 'p4_mdp-530.pcap', 'p4_mdp-1024.pcap', 'p4_mid-packet.pcap']:
    frames += pcap_frames(os.path.join(raw, fn))
for t in (30, 47, 48, 51):
    frames.append(udp_frame(open(os.path.join(raw, f'template_{t}.bin'), 'rb').read()))
with open(os.path.join(dest, 'cme_all.pcap'), 'wb') as out:
    out.write(struct.pack('<IHHiIII', 0xa1b2c3d4, 2, 4, 0, 0, 65535, 1))
    for i, fr in enumerate(frames):
        out.write(struct.pack('<IIII', 1700000000, i * 1000, len(fr), len(fr)) + fr)
print(f'{len(frames)} packets -> {dest}/cme_all.pcap')
PY
cat > "$DEST/framing.yaml" <<'YAML'
{packet_header: [{name: seq, type: u32le}, {name: sending_time, type: u64le}], message_prefix: [{name: len, type: u16le}], sequence: packet_header.seq, length: message_prefix.len, length_includes_prefix: true}
YAML
echo "ok: $DEST"
