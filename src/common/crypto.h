// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace tq::crypto {

using Sha256Digest = std::array<uint8_t, 32>;

class Sha256 {
public:
    Sha256();
    void update(const void* data, size_t len);
    void update(std::string_view s) { update(s.data(), s.size()); }
    Sha256Digest finish();

private:
    void transform(const uint8_t* block);

    uint32_t state_[8];
    uint64_t bitLen_ = 0;
    uint8_t buffer_[64];
    size_t bufferLen_ = 0;
};

std::string sha256(std::string_view data);                         // raw 32 bytes
std::string hmacSha256(std::string_view key, std::string_view msg);  // raw 32 bytes
std::string pbkdf2Sha256(std::string_view password, std::string_view salt, uint32_t iterations,
                         size_t keyLen);

uint32_t crc32c(const void* data, size_t len, uint32_t seed = 0);
inline uint32_t crc32c(std::string_view s, uint32_t seed = 0) { return crc32c(s.data(), s.size(), seed); }

// 32-bit MurmurHash3 (x86_32), used for consistent hashing and partitioning.
uint32_t murmur3(std::string_view data, uint32_t seed = 0);
uint64_t fnv1a64(std::string_view data);

std::string base64Encode(std::string_view data);
std::optional<std::string> base64Decode(std::string_view text);

// Compares two secrets without leaking where they differ.
bool constantTimeEquals(std::string_view a, std::string_view b);

}  // namespace tq::crypto
