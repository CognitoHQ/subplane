// SPDX-License-Identifier: Apache-2.0
#pragma once

// SHA-256 (FIPS 180-4), for the hashes a recording's CONTEXT carries
// (recording-format.md). Header-only, C++20, cold path: a partition's YAML and
// schema are hashed once, at agent start. Not for anything secret.

#include <array>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace subplane::record {

using Sha256Digest = std::array<uint8_t, 32>;

inline Sha256Digest sha256(const void* data, size_t len) {
    static constexpr uint32_t k[64] = {
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};
    uint32_t h[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
    const auto rotr = [](uint32_t x, unsigned n) { return (x >> n) | (x << (32 - n)); };
    const auto block = [&](const uint8_t* p) {
        uint32_t w[64];
        for (int i = 0; i < 16; ++i)
            w[i] = (uint32_t{p[4 * i]} << 24) | (uint32_t{p[4 * i + 1]} << 16) | (uint32_t{p[4 * i + 2]} << 8) | p[4 * i + 3];
        for (int i = 16; i < 64; ++i) {
            const uint32_t s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
            const uint32_t s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16] + s0 + w[i - 7] + s1;
        }
        uint32_t a = h[0], b = h[1], c = h[2], d = h[3], e = h[4], f = h[5], g = h[6], hh = h[7];
        for (int i = 0; i < 64; ++i) {
            const uint32_t t1 = hh + (rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25)) + ((e & f) ^ (~e & g)) + k[i] + w[i];
            const uint32_t t2 = (rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22)) + ((a & b) ^ (a & c) ^ (b & c));
            hh = g; g = f; f = e; e = d + t1; d = c; c = b; b = a; a = t1 + t2;
        }
        h[0] += a; h[1] += b; h[2] += c; h[3] += d; h[4] += e; h[5] += f; h[6] += g; h[7] += hh;
    };
    const auto* p = static_cast<const uint8_t*>(data);
    size_t n = len;
    for (; n >= 64; n -= 64, p += 64) block(p);
    uint8_t tail[128] = {};
    std::memcpy(tail, p, n);
    tail[n] = 0x80;
    const size_t tail_len = n < 56 ? 64 : 128;
    const uint64_t bits = uint64_t{len} * 8u;
    for (int i = 0; i < 8; ++i) tail[tail_len - 1 - static_cast<size_t>(i)] = static_cast<uint8_t>(bits >> (8 * i));
    block(tail);
    if (tail_len == 128) block(tail + 64);
    Sha256Digest out{};
    for (int i = 0; i < 8; ++i) {
        out[static_cast<size_t>(4 * i)] = static_cast<uint8_t>(h[i] >> 24);
        out[static_cast<size_t>(4 * i + 1)] = static_cast<uint8_t>(h[i] >> 16);
        out[static_cast<size_t>(4 * i + 2)] = static_cast<uint8_t>(h[i] >> 8);
        out[static_cast<size_t>(4 * i + 3)] = static_cast<uint8_t>(h[i]);
    }
    return out;
}

inline Sha256Digest sha256(std::string_view s) { return sha256(s.data(), s.size()); }

inline std::string to_hex(const Sha256Digest& d) {
    static constexpr char hex[] = "0123456789abcdef";
    std::string s(64, '0');
    for (size_t i = 0; i < 32; ++i) { s[2 * i] = hex[d[i] >> 4]; s[2 * i + 1] = hex[d[i] & 15]; }
    return s;
}

} // namespace subplane::record
