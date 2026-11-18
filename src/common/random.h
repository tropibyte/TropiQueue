// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <string>

namespace tq::rnd {

// Bytes from the operating system's entropy source.
std::string secureBytes(size_t n);
uint64_t secureU64();

// URL-safe identifier suitable for server-generated names such as
// "amq.gen-..." queue names and direct reply-to addresses.
std::string uniqueName(const std::string& prefix, size_t randomChars = 22);

// RFC 4122 version 4 UUID string.
std::string uuid4();

// Uniform integer in [lo, hi] from a fast per-thread generator. Not for secrets.
int64_t uniform(int64_t lo, int64_t hi);

}  // namespace tq::rnd
