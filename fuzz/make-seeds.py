#!/usr/bin/env python3
"""Writes the seed corpus of the fuzz targets (fuzz/corpus/<target>/seed-*): valid inputs
that reach deep into each target, for libFuzzer to mutate. Rerun after changing it:

    python3 fuzz/make-seeds.py
"""
import json
import os
import zlib

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "corpus")


def write(target, name, data):
    os.makedirs(os.path.join(ROOT, target), exist_ok=True)
    with open(os.path.join(ROOT, target, "seed-" + name), "wb") as f:
        f.write(data)


def deflate(data):
    """permessage-deflate: raw deflate, sync flush, the 4-byte tail removed."""
    c = zlib.compressobj(wbits=-15)
    out = c.compress(data) + c.flush(zlib.Z_SYNC_FLUSH)
    assert out.endswith(b"\x00\x00\xff\xff")
    return out[:-4]


def frame(payload, opcode=1, fin=True, rsv1=False, mask=b"\x12\x34\x56\x78"):
    """A masked client frame."""
    out = bytearray([(0x80 if fin else 0) | (0x40 if rsv1 else 0) | opcode])
    n = len(payload)
    if n < 126:
        out.append(0x80 | n)
    elif n < 65536:
        out += bytes([0x80 | 126]) + n.to_bytes(2, "big")
    else:
        out += bytes([0x80 | 127]) + n.to_bytes(8, "big")
    out += mask
    out += bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return bytes(out)


def announce(info_hash, peer_id, offers=0, **extra):
    m = {"action": "announce", "info_hash": info_hash, "peer_id": peer_id, "numwant": 10}
    m["offers"] = [
        {"offer": {"type": "offer", "sdp": f"v=0 sdp-{peer_id}-{i}"}, "offer_id": f"{peer_id}-o{i}"}
        for i in range(offers)
    ]
    m.update(extra)
    return json.dumps(m, separators=(",", ":")).encode()


H = "hfuzz000000000000001"
answer = json.dumps(
    {"action": "announce", "info_hash": H, "peer_id": "pa", "to_peer_id": "pb",
     "answer": {"type": "answer", "sdp": "v=0 answer"}, "offer_id": "pb-o0"},
    separators=(",", ":")).encode()

# ws_frames: control byte (bit 0 deflate, bits 1-2 limit, bits 3-7 chunk size), client frames.
write("ws_frames", "text", bytes([0b0000_0100]) + frame(announce(H, "pa", 2)))
write("ws_frames", "fragmented-ping",
      bytes([0b0001_0100]) + frame(b'{"action":', fin=False) + frame(b"x", opcode=9)
      + frame(b'"scrape"}', opcode=0))
write("ws_frames", "compressed",
      bytes([0b0010_0101]) + frame(deflate(announce(H, "pa", 3)), rsv1=True))
compressed = deflate(announce(H, "pb", 5))
write("ws_frames", "compressed-fragmented",
      bytes([0b0000_0101]) + frame(compressed[:20], fin=False, rsv1=True)
      + frame(compressed[20:], opcode=0))
write("ws_frames", "close", bytes([0b0000_0100]) + frame(b"\x03\xe8bye", opcode=8))
write("ws_frames", "binary-16bit-length", bytes([0b0000_0100]) + frame(b"b" * 300, opcode=2))

# deflate_roundtrip: window byte, message.
write("deflate_roundtrip", "announce", bytes([6]) + announce(H, "pa", 4))
write("deflate_roundtrip", "binary", bytes([0]) + bytes(range(256)) * 4)

# http_upgrade: request heads (and extension headers alone).
request = (
    "GET /announce?x=1 HTTP/1.1\r\nHost: tracker.example\r\nUpgrade: websocket\r\n"
    "Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    "Sec-WebSocket-Version: 13\r\nOrigin: https://example.com\r\n"
    "Sec-WebSocket-Protocol: chat\r\n"
    "Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n"
    "Sec-WebSocket-Extensions: permessage-deflate; server_max_window_bits=10\r\n\r\n"
)
write("http_upgrade", "upgrade", request.encode())
write("http_upgrade", "stats", b"GET /stats.json HTTP/1.1\r\nHost: x\r\n\r\n")
write("http_upgrade", "stats-infohash",
      b"GET /stats.json?infoHash=68736f6d65ff00&x=1 HTTP/1.1\r\nHost: x\r\n\r\n")
write("http_upgrade", "metrics", b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1:9100\r\n\r\n")
write("http_upgrade", "swarms", b"GET /swarms?top=25 HTTP/1.1\r\nHost: 127.0.0.1:9100\r\n\r\n")
# base64("fuzz:pa:ss"): the credentials the target checks against; then a wrong scheme.
write("http_upgrade", "metrics-auth",
      b"GET /metrics HTTP/1.1\r\nHost: x\r\nAuthorization: Basic ZnV6ejpwYTpzcw==\r\n\r\n")
write("http_upgrade", "metrics-auth-bad",
      b"GET /metrics HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer ZnV6ejpwYTpzcw==\r\n\r\n")
write("http_upgrade", "extensions",
      b"permessage-deflate; server_no_context_takeover; client_max_window_bits=12, x-webkit-deflate-frame")

# protocol: offer selection, max_offers, then 0xFF-separated frames (first byte: connection and
# kind; 0xE0 | conn = disconnect, 0xC0 = expiry).
def frames(*items):
    return b"\xff".join(items)


write("protocol", "swarm", bytes([0, 5]) + frames(
    b"\x00" + announce(H, "pa", 2),
    b"\x01" + announce(H, "pb", 2, event="started"),
    b"\x00" + answer,
    b"\x02" + json.dumps({"action": "scrape", "info_hash": [H, "nope"]}).encode(),
    b"\x01" + announce(H, "pb", 0, event="completed"),
    b"\x01" + json.dumps({"action": "announce", "event": "stopped", "info_hash": H, "peer_id": "pb"}).encode(),
    b"\xe0",
    b"\xc0",
))
write("protocol", "multi-swarm", bytes([1, 19]) + frames(
    b"\x03" + announce(H, "pc", 1),
    b"\x03" + announce("hfuzz000000000000002", "pc", 1),
    b"\x04" + announce("hfuzz000000000000002", "pd", 3),
    b"\x05" + json.dumps({"action": "scrape"}).encode(),
    b"\x04" + b"{ not json",
    b"\xe3",
))
# CI crash 2026-10-05: an info_hash equal to serde_json's reserved RawValue key (valid JSON).
write("protocol", "reserved-key", bytes([0, 5]) + frames(
    b"\x01" + announce("$serde_json::private::RawValue", "pa", 1),
    b"\x02" + json.dumps({"action": "scrape", "info_hash": ["$serde_json::private::RawValue"]}).encode(),
))
print("seeds written to", ROOT)
