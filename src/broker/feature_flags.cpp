// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/feature_flags.h"

namespace tq::broker {

const char* flagStabilityName(FlagStability s) {
    switch (s) {
        case FlagStability::Required: return "required";
        case FlagStability::Stable: return "stable";
        case FlagStability::Experimental: return "experimental";
    }
    return "stable";
}

const std::vector<FeatureFlag>& FeatureFlags::known() {
    static const std::vector<FeatureFlag> flags = {
        {"quorum_queue", "Raft-replicated quorum queues", FlagStability::Required, {}, ""},
        {"stream_queue", "Append-only stream queues", FlagStability::Required, {}, ""},
        {"implicit_default_bindings", "Default exchange bindings are not stored in the metadata",
         FlagStability::Required, {}, ""},
        {"virtual_host_metadata", "Tracing flag and limits stored with virtual hosts", FlagStability::Stable, {}, ""},
        {"user_limits", "Per-user connection and channel limits", FlagStability::Stable, {}, ""},
        {"runtime_parameters", "Shovel and federation runtime parameters", FlagStability::Stable, {}, ""},
        {"stream_protocol", "Binary stream protocol publisher deduplication", FlagStability::Stable, {"stream_queue"},
         ""},
        {"oauth2_token_sessions", "Connections authenticated with OAuth 2.0 access tokens", FlagStability::Stable, {},
         ""},
        {"detailed_queue_metrics", "Per-object metrics on /metrics/detailed", FlagStability::Experimental,
         {"virtual_host_metadata"}, ""},
    };
    return flags;
}

const FeatureFlag* FeatureFlags::find(const std::string& name) {
    for (const auto& f : known()) {
        if (f.name == name) return &f;
    }
    return nullptr;
}

void FeatureFlags::initFresh() {
    std::lock_guard<std::mutex> lock(mu_);
    enabled_.clear();
    for (const auto& f : known()) {
        if (f.stability != FlagStability::Experimental) enabled_.insert(f.name);
    }
}

void FeatureFlags::initUpgraded() {
    std::lock_guard<std::mutex> lock(mu_);
    enabled_.clear();
    for (const auto& f : known()) {
        if (f.stability == FlagStability::Required) enabled_.insert(f.name);
    }
}

bool FeatureFlags::isEnabled(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mu_);
    return enabled_.count(name) != 0;
}

bool FeatureFlags::enableLocked(const std::string& name, std::set<std::string>& visiting, std::string* error) {
    if (enabled_.count(name)) return true;
    const FeatureFlag* f = find(name);
    if (!f) {
        if (error) *error = "unknown feature flag '" + name + "'";
        return false;
    }
    if (!visiting.insert(name).second) {
        if (error) *error = "feature flag dependency cycle at '" + name + "'";
        return false;
    }
    for (const auto& dep : f->dependsOn) {
        if (!enableLocked(dep, visiting, error)) return false;
    }
    enabled_.insert(name);
    return true;
}

bool FeatureFlags::enable(const std::string& name, const std::set<std::string>& supportedByPeers, std::string* error) {
    const FeatureFlag* f = find(name);
    if (!f) {
        if (error) *error = "unknown feature flag '" + name + "'";
        return false;
    }
    if (!supportedByPeers.empty() && !supportedByPeers.count(name)) {
        if (error) *error = "feature flag '" + name + "' is not supported by every cluster member";
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    std::set<std::string> visiting;
    std::set<std::string> before = enabled_;
    if (!enableLocked(name, visiting, error)) {
        enabled_ = std::move(before);
        return false;
    }
    return true;
}

std::vector<std::string> FeatureFlags::enabled() const {
    std::lock_guard<std::mutex> lock(mu_);
    return {enabled_.begin(), enabled_.end()};
}

void FeatureFlags::load(const Json& names) {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& f : known()) {
        if (f.stability == FlagStability::Required) enabled_.insert(f.name);
    }
    for (const auto& n : names.asArray()) {
        if (find(n.asString())) enabled_.insert(n.asString());
    }
}

Json FeatureFlags::toJson() const {
    std::lock_guard<std::mutex> lock(mu_);
    Json arr = Json::array();
    for (const auto& f : known()) {
        Json j = Json::object();
        j["name"] = f.name;
        j["desc"] = f.description;
        j["state"] = enabled_.count(f.name) ? "enabled" : "disabled";
        j["stability"] = flagStabilityName(f.stability);
        Json deps = Json::array();
        for (const auto& d : f.dependsOn) deps.push(d);
        j["depends_on"] = deps;
        arr.push(j);
    }
    return arr;
}

}  // namespace tq::broker
