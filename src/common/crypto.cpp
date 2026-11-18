// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "common/crypto.h"

#include <algorithm>
#include <cstring>

namespace tq::crypto {

namespace {

constexpr uint32_t kSha256K[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

inline uint32_t rotr(uint32_t x, int n) { return (x >> n) | (x << (32 - n)); }
inline uint32_t rotl(uint32_t x, int n) { return (x << n) | (x >> (32 - n)); }

uint32_t g_crcTable[8][256];
bool g_crcInit = false;

void initCrcTable() {
    const uint32_t poly = 0x82F63B78u;  // CRC-32C (Castagnoli), reflected
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) c = (c & 1) ? (c >> 1) ^ poly : (c >> 1);
        g_crcTable[0][i] = c;
    }
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = g_crcTable[0][i];
        for (int t = 1; t < 8; ++t) {
            c = g_crcTable[0][c & 0xFF] ^ (c >> 8);
            g_crcTable[t][i] = c;
        }
    }
    g_crcInit = true;
}

struct CrcInit {
    CrcInit() { initCrcTable(); }
} g_crcInitOnce;

}  // namespace

// -------------------------------------------------------------------- SHA-256

Sha256::Sha256() {
    static const uint32_t init[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                                     0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
    std::memcpy(state_, init, sizeof state_);
}

void Sha256::transform(const uint8_t* block) {
    uint32_t w[64];
    for (int i = 0; i < 16; ++i) {
        w[i] = (static_cast<uint32_t>(block[i * 4]) << 24) | (static_cast<uint32_t>(block[i * 4 + 1]) << 16) |
               (static_cast<uint32_t>(block[i * 4 + 2]) << 8) | static_cast<uint32_t>(block[i * 4 + 3]);
    }
    for (int i = 16; i < 64; ++i) {
        uint32_t s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
        uint32_t s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }
    uint32_t a = state_[0], b = state_[1], c = state_[2], d = state_[3];
    uint32_t e = state_[4], f = state_[5], g = state_[6], h = state_[7];
    for (int i = 0; i < 64; ++i) {
        uint32_t S1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
        uint32_t ch = (e & f) ^ (~e & g);
        uint32_t t1 = h + S1 + ch + kSha256K[i] + w[i];
        uint32_t S0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
        uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
        uint32_t t2 = S0 + maj;
        h = g; g = f; f = e; e = d + t1;
        d = c; c = b; b = a; a = t1 + t2;
    }
    state_[0] += a; state_[1] += b; state_[2] += c; state_[3] += d;
    state_[4] += e; state_[5] += f; state_[6] += g; state_[7] += h;
}

void Sha256::update(const void* data, size_t len) {
    const auto* p = static_cast<const uint8_t*>(data);
    bitLen_ += static_cast<uint64_t>(len) * 8;
    while (len > 0) {
        size_t take = std::min(len, sizeof(buffer_) - bufferLen_);
        std::memcpy(buffer_ + bufferLen_, p, take);
        bufferLen_ += take;
        p += take;
        len -= take;
        if (bufferLen_ == 64) {
            transform(buffer_);
            bufferLen_ = 0;
        }
    }
}

Sha256Digest Sha256::finish() {
    uint64_t bits = bitLen_;
    uint8_t pad = 0x80;
    update(&pad, 1);
    uint8_t zero = 0;
    while (bufferLen_ != 56) update(&zero, 1);
    uint8_t lenBytes[8];
    for (int i = 0; i < 8; ++i) lenBytes[i] = static_cast<uint8_t>(bits >> (56 - i * 8));
    update(lenBytes, 8);
    Sha256Digest out{};
    for (int i = 0; i < 8; ++i) {
        out[i * 4] = static_cast<uint8_t>(state_[i] >> 24);
        out[i * 4 + 1] = static_cast<uint8_t>(state_[i] >> 16);
        out[i * 4 + 2] = static_cast<uint8_t>(state_[i] >> 8);
        out[i * 4 + 3] = static_cast<uint8_t>(state_[i]);
    }
    return out;
}

std::string sha256(std::string_view data) {
    Sha256 h;
    h.update(data);
    auto d = h.finish();
    return std::string(reinterpret_cast<const char*>(d.data()), d.size());
}

std::string hmacSha256(std::string_view key, std::string_view msg) {
    std::string k(key);
    if (k.size() > 64) k = sha256(k);
    k.resize(64, '\0');
    std::string ipad(64, '\0'), opad(64, '\0');
    for (size_t i = 0; i < 64; ++i) {
        ipad[i] = static_cast<char>(k[i] ^ 0x36);
        opad[i] = static_cast<char>(k[i] ^ 0x5c);
    }
    Sha256 inner;
    inner.update(ipad);
    inner.update(msg);
    auto innerDigest = inner.finish();
    Sha256 outer;
    outer.update(opad);
    outer.update(innerDigest.data(), innerDigest.size());
    auto d = outer.finish();
    return std::string(reinterpret_cast<const char*>(d.data()), d.size());
}

std::string pbkdf2Sha256(std::string_view password, std::string_view salt, uint32_t iterations,
                         size_t keyLen) {
    std::string out;
    uint32_t block = 1;
    while (out.size() < keyLen) {
        std::string s(salt);
        s.push_back(static_cast<char>(block >> 24));
        s.push_back(static_cast<char>(block >> 16));
        s.push_back(static_cast<char>(block >> 8));
        s.push_back(static_cast<char>(block));
        std::string u = hmacSha256(password, s);
        std::string t = u;
        for (uint32_t i = 1; i < iterations; ++i) {
            u = hmacSha256(password, u);
            for (size_t j = 0; j < t.size(); ++j) t[j] = static_cast<char>(t[j] ^ u[j]);
        }
        out.append(t);
        ++block;
    }
    out.resize(keyLen);
    return out;
}

// --------------------------------------------------------------------- CRC32C

uint32_t crc32c(const void* data, size_t len, uint32_t seed) {
    if (!g_crcInit) initCrcTable();
    const auto* p = static_cast<const uint8_t*>(data);
    uint32_t crc = ~seed;
    while (len >= 8) {
        uint32_t lo = crc ^ (static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
                             (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24));
        uint32_t hi = static_cast<uint32_t>(p[4]) | (static_cast<uint32_t>(p[5]) << 8) |
                      (static_cast<uint32_t>(p[6]) << 16) | (static_cast<uint32_t>(p[7]) << 24);
        crc = g_crcTable[7][lo & 0xFF] ^ g_crcTable[6][(lo >> 8) & 0xFF] ^ g_crcTable[5][(lo >> 16) & 0xFF] ^
              g_crcTable[4][lo >> 24] ^ g_crcTable[3][hi & 0xFF] ^ g_crcTable[2][(hi >> 8) & 0xFF] ^
              g_crcTable[1][(hi >> 16) & 0xFF] ^ g_crcTable[0][hi >> 24];
        p += 8;
        len -= 8;
    }
    while (len--) crc = g_crcTable[0][(crc ^ *p++) & 0xFF] ^ (crc >> 8);
    return ~crc;
}

// -------------------------------------------------------------------- Hashing

uint32_t murmur3(std::string_view data, uint32_t seed) {
    const auto* p = reinterpret_cast<const uint8_t*>(data.data());
    const size_t len = data.size();
    const size_t nblocks = len / 4;
    uint32_t h = seed;
    const uint32_t c1 = 0xcc9e2d51, c2 = 0x1b873593;
    for (size_t i = 0; i < nblocks; ++i) {
        uint32_t k;
        std::memcpy(&k, p + i * 4, 4);
        k *= c1;
        k = rotl(k, 15);
        k *= c2;
        h ^= k;
        h = rotl(h, 13);
        h = h * 5 + 0xe6546b64;
    }
    const uint8_t* tail = p + nblocks * 4;
    uint32_t k1 = 0;
    switch (len & 3) {
        case 3: k1 ^= static_cast<uint32_t>(tail[2]) << 16; [[fallthrough]];
        case 2: k1 ^= static_cast<uint32_t>(tail[1]) << 8; [[fallthrough]];
        case 1:
            k1 ^= tail[0];
            k1 *= c1;
            k1 = rotl(k1, 15);
            k1 *= c2;
            h ^= k1;
    }
    h ^= static_cast<uint32_t>(len);
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

uint64_t fnv1a64(std::string_view data) {
    uint64_t h = 0xcbf29ce484222325ULL;
    for (unsigned char c : data) {
        h ^= c;
        h *= 0x100000001b3ULL;
    }
    return h;
}

// --------------------------------------------------------------------- Base64

static const char kB64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string base64Encode(std::string_view data) {
    std::string out;
    out.reserve((data.size() + 2) / 3 * 4);
    size_t i = 0;
    while (i + 3 <= data.size()) {
        uint32_t v = (static_cast<uint8_t>(data[i]) << 16) | (static_cast<uint8_t>(data[i + 1]) << 8) |
                     static_cast<uint8_t>(data[i + 2]);
        out.push_back(kB64[(v >> 18) & 63]);
        out.push_back(kB64[(v >> 12) & 63]);
        out.push_back(kB64[(v >> 6) & 63]);
        out.push_back(kB64[v & 63]);
        i += 3;
    }
    size_t rest = data.size() - i;
    if (rest == 1) {
        uint32_t v = static_cast<uint8_t>(data[i]) << 16;
        out.push_back(kB64[(v >> 18) & 63]);
        out.push_back(kB64[(v >> 12) & 63]);
        out.append("==");
    } else if (rest == 2) {
        uint32_t v = (static_cast<uint8_t>(data[i]) << 16) | (static_cast<uint8_t>(data[i + 1]) << 8);
        out.push_back(kB64[(v >> 18) & 63]);
        out.push_back(kB64[(v >> 12) & 63]);
        out.push_back(kB64[(v >> 6) & 63]);
        out.push_back('=');
    }
    return out;
}

std::optional<std::string> base64Decode(std::string_view text) {
    auto val = [](char c) -> int {
        if (c >= 'A' && c <= 'Z') return c - 'A';
        if (c >= 'a' && c <= 'z') return c - 'a' + 26;
        if (c >= '0' && c <= '9') return c - '0' + 52;
        if (c == '+') return 62;
        if (c == '/') return 63;
        return -1;
    };
    std::string out;
    uint32_t acc = 0;
    int bits = 0;
    size_t padding = 0;
    for (char c : text) {
        if (c == '=') {
            ++padding;
            continue;
        }
        if (c == '\r' || c == '\n') continue;
        if (padding) return std::nullopt;
        int v = val(c);
        if (v < 0) return std::nullopt;
        acc = (acc << 6) | static_cast<uint32_t>(v);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<char>((acc >> bits) & 0xFF));
        }
    }
    if (padding > 2) return std::nullopt;
    return out;
}

bool constantTimeEquals(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (a[i] != b[i]) return false;
    }
    return true;
}

}  // namespace tq::crypto
