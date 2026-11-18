// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace tq {

// Wall-clock milliseconds since the Unix epoch.
inline int64_t wallClockMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// Monotonic milliseconds, unaffected by wall-clock adjustments.
inline int64_t monotonicMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

// Abstract time source so expiry logic can be tested deterministically.
class Clock {
public:
    virtual ~Clock() = default;
    virtual int64_t nowMs() const = 0;        // wall clock
    virtual int64_t monotonic() const = 0;    // steady clock
};

class SystemClock : public Clock {
public:
    int64_t nowMs() const override { return wallClockMs(); }
    int64_t monotonic() const override { return monotonicMs(); }
    static SystemClock& instance() {
        static SystemClock c;
        return c;
    }
};

class ManualClock : public Clock {
public:
    explicit ManualClock(int64_t start = 1'700'000'000'000) : now_(start), mono_(1000) {}
    int64_t nowMs() const override { return now_.load(); }
    int64_t monotonic() const override { return mono_.load(); }
    void advance(int64_t ms) {
        now_ += ms;
        mono_ += ms;
    }

private:
    std::atomic<int64_t> now_;
    std::atomic<int64_t> mono_;
};

}  // namespace tq
