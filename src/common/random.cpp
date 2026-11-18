// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "common/random.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <random>
#include <thread>

namespace tq::rnd {

namespace {

std::mt19937_64& fastEngine() {
    thread_local std::mt19937_64 engine(
        static_cast<uint64_t>(std::chrono::system_clock::now().time_since_epoch().count()) ^
        std::hash<std::thread::id>{}(std::this_thread::get_id()));
    return engine;
}

std::atomic<uint64_t> g_nameCounter{0};

const char kAlphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

}  // namespace

std::string secureBytes(size_t n) {
    std::random_device rd;
    std::string out;
    out.reserve(n);
    while (out.size() < n) {
        uint32_t v = rd();
        for (int i = 0; i < 4 && out.size() < n; ++i) out.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
    }
    return out;
}

uint64_t secureU64() {
    std::random_device rd;
    return (static_cast<uint64_t>(rd()) << 32) | rd();
}

std::string uniqueName(const std::string& prefix, size_t randomChars) {
    auto& engine = fastEngine();
    uint64_t seq = g_nameCounter.fetch_add(1, std::memory_order_relaxed);
    std::string out = prefix;
    out.reserve(prefix.size() + randomChars);
    uint64_t bits = engine() ^ seq;
    int avail = 64;
    for (size_t i = 0; i < randomChars; ++i) {
        if (avail < 6) {
            bits = engine();
            avail = 64;
        }
        out.push_back(kAlphabet[bits & 63]);
        bits >>= 6;
        avail -= 6;
    }
    return out;
}

std::string uuid4() {
    std::string b = secureBytes(16);
    b[6] = static_cast<char>((static_cast<uint8_t>(b[6]) & 0x0F) | 0x40);
    b[8] = static_cast<char>((static_cast<uint8_t>(b[8]) & 0x3F) | 0x80);
    char buf[37];
    const auto* u = reinterpret_cast<const uint8_t*>(b.data());
    std::snprintf(buf, sizeof buf, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x", u[0],
                  u[1], u[2], u[3], u[4], u[5], u[6], u[7], u[8], u[9], u[10], u[11], u[12], u[13], u[14], u[15]);
    return buf;
}

int64_t uniform(int64_t lo, int64_t hi) {
    std::uniform_int_distribution<int64_t> dist(lo, hi);
    return dist(fastEngine());
}

}  // namespace tq::rnd
