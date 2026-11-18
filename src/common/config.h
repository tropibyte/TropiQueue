// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace tq {

// Flat key/value configuration in the "sysctl" style used by tropiqueue.conf:
//
//   listeners.tcp.default = 5672
//   vm_memory_high_watermark.relative = 0.4
//   # comments start with '#'
//
// Values may reference environment variables as $(NAME).
class Config {
public:
    Config() = default;

    static Config fromFile(const std::string& path, std::vector<std::string>* errors = nullptr);
    static Config fromString(const std::string& text, std::vector<std::string>* errors = nullptr);

    void set(const std::string& key, const std::string& value) { values_[key] = value; }
    bool has(const std::string& key) const { return values_.count(key) != 0; }
    void erase(const std::string& key) { values_.erase(key); }

    std::string getString(const std::string& key, const std::string& def = "") const;
    int64_t getInt(const std::string& key, int64_t def) const;
    double getDouble(const std::string& key, double def) const;
    bool getBool(const std::string& key, bool def) const;
    int64_t getBytes(const std::string& key, int64_t def) const;
    int64_t getDurationMs(const std::string& key, int64_t def) const;
    std::vector<std::string> getList(const std::string& key) const;

    // All keys that start with prefix, e.g. "listeners.tcp.".
    std::map<std::string, std::string> withPrefix(const std::string& prefix) const;
    const std::map<std::string, std::string>& all() const { return values_; }

    // Command-line overrides of the form --key=value.
    void applyOverrides(const std::vector<std::string>& args);

private:
    std::map<std::string, std::string> values_;
};

std::string expandEnvironment(const std::string& value);

}  // namespace tq
