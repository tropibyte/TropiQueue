// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Runtime parameters: per-vhost JSON values grouped by component, used by
// plugins-style features such as dynamic shovels ("shovel") and federation
// ("federation-upstream", "federation-upstream-set"). Global parameters
// (e.g. "cluster_name") are not tied to a vhost.
//
// Components register a validator so bad values are rejected when they are
// set, and listeners that are told about every change.
#pragma once

#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "common/json.h"

namespace tq::broker {

struct RuntimeParameter {
    std::string component;
    std::string vhost;
    std::string name;
    Json value;

    Json toJson() const;
    static std::optional<RuntimeParameter> fromJson(const Json& j, std::string* error);
};

class ParameterStore {
public:
    using Validator = std::function<bool(const std::string& vhost, const std::string& name, const Json& value,
                                         std::string* error)>;
    using Listener = std::function<void(const RuntimeParameter& param, bool removed)>;

    void registerComponent(const std::string& component, Validator validator);
    bool knownComponent(const std::string& component) const;
    std::vector<std::string> components() const;

    // Listeners are called after the change, without internal locks held.
    void addListener(const std::string& component, Listener listener);

    bool set(const RuntimeParameter& p, std::string* error);
    bool clear(const std::string& component, const std::string& vhost, const std::string& name);
    std::optional<RuntimeParameter> get(const std::string& component, const std::string& vhost,
                                        const std::string& name) const;
    std::vector<RuntimeParameter> list(const std::string& component = "", const std::string& vhost = "") const;
    void clearVhost(const std::string& vhost);

    // ----- global parameters
    void setGlobal(const std::string& name, const Json& value);
    bool clearGlobal(const std::string& name);
    std::optional<Json> global(const std::string& name) const;
    std::map<std::string, Json> globals() const;

    // The cluster name used by federation loop detection and the overview.
    std::string clusterName(const std::string& fallback) const;

private:
    void notify(const RuntimeParameter& p, bool removed);

    mutable std::mutex mu_;
    std::map<std::string, Validator> validators_;
    std::map<std::string, std::vector<Listener>> listeners_;
    std::map<std::tuple<std::string, std::string, std::string>, RuntimeParameter> params_;
    std::map<std::string, Json> globals_;
};

}  // namespace tq::broker
