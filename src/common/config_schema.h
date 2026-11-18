// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// The set of configuration keys tqd understands, with their value types,
// defaults and documentation. Used to validate tropiqueue.conf at startup
// (typos in key names are otherwise silently ignored) and to generate the
// key reference printed by `tqd --list-config-keys`.
#pragma once

#include <string>
#include <vector>

#include "common/config.h"

namespace tq {

enum class ConfigType {
    String,
    Bool,
    Int,
    Port,
    Double,
    Fraction,  // 0.0 .. 1.0
    Bytes,     // 128MB, 1GiB, 4096
    Duration,  // 30s, 5m, 250ms, 1h
    Endpoint,  // port, host:port, [v6]:port
    List,      // comma separated
    Enum,      // one of `choices`
    LogLevel,
};

struct ConfigKey {
    std::string name;  // may end in ".*" for a family of keys
    ConfigType type = ConfigType::String;
    std::string defaultValue;
    std::string description;
    std::vector<std::string> choices;  // Enum only
    bool secret = false;               // never echo the value
};

struct ConfigIssue {
    enum class Level { Warning, Error };
    Level level = Level::Warning;
    std::string key;
    std::string message;
};

class ConfigSchema {
public:
    // The schema of tropiqueue.conf.
    static const ConfigSchema& broker();

    explicit ConfigSchema(std::vector<ConfigKey> keys);

    const ConfigKey* find(const std::string& key) const;
    const std::vector<ConfigKey>& keys() const { return keys_; }

    // Checks every key of `cfg`: unknown keys (with a "did you mean" hint)
    // are warnings, values that do not parse as their type are errors.
    std::vector<ConfigIssue> validate(const Config& cfg) const;

    // Checks one value against a key's type. Returns an empty string when the
    // value is valid, otherwise a description of the problem.
    static std::string checkValue(const ConfigKey& key, const std::string& value);

    // Markdown reference of every key.
    std::string reference() const;

    // A sample configuration file with every key commented out.
    std::string sampleConfig() const;

private:
    std::string suggest(const std::string& unknown) const;

    std::vector<ConfigKey> keys_;
};

const char* configTypeName(ConfigType t);

// Levenshtein distance, used for "did you mean" suggestions.
size_t editDistance(const std::string& a, const std::string& b);

}  // namespace tq
