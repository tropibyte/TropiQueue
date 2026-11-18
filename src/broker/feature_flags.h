// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Feature flags let a cluster run nodes of different versions during a
// rolling upgrade: behaviour that older nodes do not understand stays off
// until an operator enables the flag, which is only possible once every node
// supports it. Enabled flags are part of the replicated metadata and can
// never be disabled again.
//
// A fresh node enables every stable flag; a node upgraded from an older
// metadata snapshot starts with only the required flags enabled.
#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "common/json.h"

namespace tq::broker {

enum class FlagStability { Required, Stable, Experimental };
const char* flagStabilityName(FlagStability s);

struct FeatureFlag {
    std::string name;
    std::string description;
    FlagStability stability = FlagStability::Stable;
    std::vector<std::string> dependsOn;
    std::string docUrl;
};

class FeatureFlags {
public:
    // The flags this version of the broker knows about.
    static const std::vector<FeatureFlag>& known();
    static const FeatureFlag* find(const std::string& name);

    // Initial state for a node without stored metadata.
    void initFresh();
    // Initial state for a node whose metadata predates feature flags.
    void initUpgraded();

    bool isEnabled(const std::string& name) const;
    // Enables a flag and its dependencies. Fails for unknown flags and for
    // flags not supported by every cluster member.
    bool enable(const std::string& name, const std::set<std::string>& supportedByPeers, std::string* error);
    std::vector<std::string> enabled() const;
    void load(const Json& names);

    Json toJson() const;  // [{name, desc, state, stability, depends_on}]

private:
    bool enableLocked(const std::string& name, std::set<std::string>& visiting, std::string* error);

    mutable std::mutex mu_;
    std::set<std::string> enabled_;
};

}  // namespace tq::broker
