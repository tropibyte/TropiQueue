// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "common/config.h"

#include <cstdlib>
#include <fstream>
#include <sstream>

#include "common/logging.h"
#include "common/strings.h"

namespace tq {

std::string expandEnvironment(const std::string& value) {
    std::string out;
    size_t i = 0;
    while (i < value.size()) {
        if (value[i] == '$' && i + 1 < value.size() && value[i + 1] == '(') {
            size_t close = value.find(')', i + 2);
            if (close != std::string::npos) {
                std::string name = value.substr(i + 2, close - i - 2);
                const char* env = std::getenv(name.c_str());
                if (env) out.append(env);
                i = close + 1;
                continue;
            }
        }
        out.push_back(value[i++]);
    }
    return out;
}

Config Config::fromString(const std::string& text, std::vector<std::string>* errors) {
    Config cfg;
    std::istringstream in(text);
    std::string line;
    int lineNo = 0;
    while (std::getline(in, line)) {
        ++lineNo;
        std::string t = str::trim(line);
        if (t.empty() || t[0] == '#') continue;
        size_t eq = t.find('=');
        if (eq == std::string::npos) {
            if (errors) errors->push_back("line " + std::to_string(lineNo) + ": expected 'key = value'");
            continue;
        }
        std::string key = str::trim(t.substr(0, eq));
        std::string value = str::trim(t.substr(eq + 1));
        if (key.empty()) {
            if (errors) errors->push_back("line " + std::to_string(lineNo) + ": empty key");
            continue;
        }
        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
            value = value.substr(1, value.size() - 2);
        }
        cfg.values_[key] = expandEnvironment(value);
    }
    return cfg;
}

Config Config::fromFile(const std::string& path, std::vector<std::string>* errors) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        if (errors) errors->push_back("cannot open config file " + path);
        return Config{};
    }
    std::stringstream ss;
    ss << in.rdbuf();
    return fromString(ss.str(), errors);
}

std::string Config::getString(const std::string& key, const std::string& def) const {
    auto it = values_.find(key);
    return it == values_.end() ? def : it->second;
}

int64_t Config::getInt(const std::string& key, int64_t def) const {
    auto it = values_.find(key);
    if (it == values_.end()) return def;
    auto v = str::parseInt(it->second);
    if (!v) {
        LOG_WARN("config") << "invalid integer for " << key << ": " << it->second;
        return def;
    }
    return *v;
}

double Config::getDouble(const std::string& key, double def) const {
    auto it = values_.find(key);
    if (it == values_.end()) return def;
    auto v = str::parseDouble(it->second);
    if (!v) {
        LOG_WARN("config") << "invalid number for " << key << ": " << it->second;
        return def;
    }
    return *v;
}

bool Config::getBool(const std::string& key, bool def) const {
    auto it = values_.find(key);
    if (it == values_.end()) return def;
    auto v = str::parseBool(it->second);
    if (!v) {
        LOG_WARN("config") << "invalid boolean for " << key << ": " << it->second;
        return def;
    }
    return *v;
}

int64_t Config::getBytes(const std::string& key, int64_t def) const {
    auto it = values_.find(key);
    if (it == values_.end()) return def;
    auto v = str::parseByteSize(it->second);
    if (!v) {
        LOG_WARN("config") << "invalid size for " << key << ": " << it->second;
        return def;
    }
    return *v;
}

int64_t Config::getDurationMs(const std::string& key, int64_t def) const {
    auto it = values_.find(key);
    if (it == values_.end()) return def;
    auto v = str::parseDurationMs(it->second);
    if (!v) {
        LOG_WARN("config") << "invalid duration for " << key << ": " << it->second;
        return def;
    }
    return *v;
}

std::vector<std::string> Config::getList(const std::string& key) const {
    auto it = values_.find(key);
    if (it == values_.end()) return {};
    return str::splitTrimmed(it->second, ',');
}

std::map<std::string, std::string> Config::withPrefix(const std::string& prefix) const {
    std::map<std::string, std::string> out;
    for (auto it = values_.lower_bound(prefix); it != values_.end(); ++it) {
        if (!str::startsWith(it->first, prefix)) break;
        out.emplace(it->first, it->second);
    }
    return out;
}

void Config::applyOverrides(const std::vector<std::string>& args) {
    for (const auto& a : args) {
        if (!str::startsWith(a, "--")) continue;
        size_t eq = a.find('=');
        if (eq == std::string::npos) continue;
        values_[a.substr(2, eq - 2)] = a.substr(eq + 1);
    }
}

}  // namespace tq
