// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace tq {

class Counter {
public:
    void inc(uint64_t n = 1) { v_.fetch_add(n, std::memory_order_relaxed); }
    uint64_t value() const { return v_.load(std::memory_order_relaxed); }

private:
    std::atomic<uint64_t> v_{0};
};

class Gauge {
public:
    void set(int64_t v) { v_.store(v, std::memory_order_relaxed); }
    void add(int64_t d) { v_.fetch_add(d, std::memory_order_relaxed); }
    int64_t value() const { return v_.load(std::memory_order_relaxed); }

private:
    std::atomic<int64_t> v_{0};
};

// Fixed-bucket latency histogram (values in microseconds or any unit).
class Histogram {
public:
    explicit Histogram(std::vector<double> bounds = defaultBounds());
    void observe(double v);
    uint64_t count() const;
    double sum() const;
    // Approximate percentile (0..100) from the bucket boundaries.
    double percentile(double p) const;
    std::vector<std::pair<double, uint64_t>> buckets() const;  // cumulative
    static std::vector<double> defaultBounds();

private:
    mutable std::mutex mu_;
    std::vector<double> bounds_;
    std::vector<uint64_t> counts_;
    uint64_t total_ = 0;
    double sum_ = 0;
};

// Rolling rate meter: events per second over the last few seconds.
class RateMeter {
public:
    void mark(uint64_t n = 1);
    double ratePerSecond() const;

private:
    mutable std::mutex mu_;
    int64_t windowStartMs_ = 0;
    uint64_t current_ = 0;
    double lastRate_ = 0;
};

// Registry of named metrics rendered in the Prometheus text exposition format.
class MetricsRegistry {
public:
    static MetricsRegistry& global();

    Counter& counter(const std::string& name, const std::string& help);
    Gauge& gauge(const std::string& name, const std::string& help);
    Histogram& histogram(const std::string& name, const std::string& help);

    std::string renderPrometheus() const;

private:
    struct Entry {
        std::string help;
        std::unique_ptr<Counter> counter;
        std::unique_ptr<Gauge> gauge;
        std::unique_ptr<Histogram> histogram;
    };
    mutable std::mutex mu_;
    std::map<std::string, Entry> entries_;
};

}  // namespace tq
