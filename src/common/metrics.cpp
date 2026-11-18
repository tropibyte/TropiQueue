// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "common/metrics.h"

#include <algorithm>
#include <cstdio>

#include "common/clock.h"

namespace tq {

Histogram::Histogram(std::vector<double> bounds) : bounds_(std::move(bounds)), counts_(bounds_.size() + 1, 0) {}

std::vector<double> Histogram::defaultBounds() {
    return {50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000, 1000000};
}

void Histogram::observe(double v) {
    std::lock_guard<std::mutex> lock(mu_);
    size_t i = static_cast<size_t>(std::lower_bound(bounds_.begin(), bounds_.end(), v) - bounds_.begin());
    counts_[i]++;
    total_++;
    sum_ += v;
}

uint64_t Histogram::count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return total_;
}

double Histogram::sum() const {
    std::lock_guard<std::mutex> lock(mu_);
    return sum_;
}

double Histogram::percentile(double p) const {
    std::lock_guard<std::mutex> lock(mu_);
    uint64_t rank = static_cast<uint64_t>(p / 100.0 * static_cast<double>(total_));
    uint64_t seen = 0;
    for (size_t i = 0; i < counts_.size(); ++i) {
        seen += counts_[i];
        if (seen > rank || seen == total_) {
            if (i == bounds_.size()) return bounds_.empty() ? sum_ / static_cast<double>(total_) : bounds_.back();
            return bounds_[i];
        }
    }
    return bounds_.empty() ? 0.0 : bounds_.back();
}

std::vector<std::pair<double, uint64_t>> Histogram::buckets() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<std::pair<double, uint64_t>> out;
    uint64_t cum = 0;
    for (size_t i = 0; i < bounds_.size(); ++i) {
        cum += counts_[i];
        out.emplace_back(bounds_[i], cum);
    }
    return out;
}

void RateMeter::mark(uint64_t n) {
    std::lock_guard<std::mutex> lock(mu_);
    int64_t now = monotonicMs();
    if (windowStartMs_ == 0) windowStartMs_ = now;
    if (now - windowStartMs_ >= 5000) {
        lastRate_ = static_cast<double>(current_) * 1000.0 / static_cast<double>(now - windowStartMs_);
        windowStartMs_ = now;
        current_ = 0;
    }
    current_ += n;
}

double RateMeter::ratePerSecond() const {
    std::lock_guard<std::mutex> lock(mu_);
    return lastRate_;
}

MetricsRegistry& MetricsRegistry::global() {
    static MetricsRegistry reg;
    return reg;
}

Counter& MetricsRegistry::counter(const std::string& name, const std::string& help) {
    std::lock_guard<std::mutex> lock(mu_);
    auto& e = entries_[name];
    if (!e.counter) {
        e.counter = std::make_unique<Counter>();
        e.help = help;
    }
    return *e.counter;
}

Gauge& MetricsRegistry::gauge(const std::string& name, const std::string& help) {
    std::lock_guard<std::mutex> lock(mu_);
    auto& e = entries_[name];
    if (!e.gauge) {
        e.gauge = std::make_unique<Gauge>();
        e.help = help;
    }
    return *e.gauge;
}

Histogram& MetricsRegistry::histogram(const std::string& name, const std::string& help) {
    std::lock_guard<std::mutex> lock(mu_);
    auto& e = entries_[name];
    if (!e.histogram) {
        e.histogram = std::make_unique<Histogram>();
        e.help = help;
    }
    return *e.histogram;
}

static std::string formatDouble(double v) {
    char buf[32];
    std::snprintf(buf, sizeof buf, "%g", v);
    return buf;
}

std::string MetricsRegistry::renderPrometheus() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::string out;
    for (const auto& [name, e] : entries_) {
        out += "# HELP " + name + " " + e.help + "\n";
        if (e.counter) {
            out += "# TYPE " + name + " counter\n";
            out += name + " " + std::to_string(e.counter->value()) + "\n";
        } else if (e.gauge) {
            out += "# TYPE " + name + " gauge\n";
            out += name + " " + std::to_string(e.gauge->value()) + "\n";
        } else if (e.histogram) {
            out += "# TYPE " + name + " histogram\n";
            for (const auto& [bound, cum] : e.histogram->buckets()) {
                out += name + "_bucket{le=\"" + formatDouble(bound) + "\"} " + std::to_string(cum) + "\n";
            }
            out += name + "_bucket{le=\"+Inf\"} " + std::to_string(e.histogram->count()) + "\n";
            out += name + "_sum " + formatDouble(e.histogram->sum()) + "\n";
            out += name + "_count " + std::to_string(e.histogram->count()) + "\n";
            out += name + "_p99 " + formatDouble(e.histogram->percentile(99)) + "\n";
        }
    }
    return out;
}

}  // namespace tq
