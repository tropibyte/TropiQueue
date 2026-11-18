// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <regex>
#include <string>
#include <vector>

#include "amqp/field_table.h"
#include "broker/queue.h"
#include "common/json.h"

namespace tq::broker {

struct Policy {
    std::string name;
    std::string vhost;
    std::string pattern;
    std::string applyTo = "all";  // all | queues | exchanges | classic_queues | quorum_queues | streams
    int priority = 0;
    Json definition = Json::object();

    Json toJson() const;
    static std::optional<Policy> fromJson(const Json& j, std::string* error);
};

// Policies attach arguments to queues and exchanges by name pattern. When
// several match, the one with the highest priority wins.
class PolicyStore {
public:
    bool set(const Policy& p, std::string* error);
    bool clear(const std::string& vhost, const std::string& name);
    void clearVhost(const std::string& vhost);
    std::vector<Policy> list(const std::string& vhost = "") const;
    std::optional<Policy> get(const std::string& vhost, const std::string& name) const;

    std::optional<Policy> matchQueue(const std::string& vhost, const std::string& name, QueueType type) const;
    std::optional<Policy> matchExchange(const std::string& vhost, const std::string& name) const;

    // Combines declared x-arguments with a policy definition.
    static amqp::FieldTable effectiveQueueArguments(const amqp::FieldTable& declared, const Policy* policy);
    static amqp::FieldTable effectiveExchangeArguments(const amqp::FieldTable& declared, const Policy* policy);

private:
    std::optional<Policy> bestMatch(const std::string& vhost, const std::string& name,
                                    bool (*applies)(const Policy&, QueueType), QueueType type) const;

    mutable std::mutex mu_;
    std::map<std::pair<std::string, std::string>, std::pair<Policy, std::regex>> policies_;
};

}  // namespace tq::broker
