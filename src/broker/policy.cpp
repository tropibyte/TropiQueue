// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/policy.h"

#include <algorithm>

namespace tq::broker {

namespace {

struct KeyMapping {
    const char* policyKey;
    const char* argument;
    bool numericMinimum;  // for limits the lower of declared/policy value wins
};

const KeyMapping kQueueKeys[] = {
    {"message-ttl", "x-message-ttl", true},
    {"expires", "x-expires", true},
    {"max-length", "x-max-length", true},
    {"max-length-bytes", "x-max-length-bytes", true},
    {"delivery-limit", "x-delivery-limit", true},
    {"overflow", "x-overflow", false},
    {"dead-letter-exchange", "x-dead-letter-exchange", false},
    {"dead-letter-routing-key", "x-dead-letter-routing-key", false},
    {"queue-mode", "x-queue-mode", false},
    {"max-age", "x-max-age", false},
    {"stream-max-segment-size-bytes", "x-stream-max-segment-size-bytes", true},
    {"single-active-consumer", "x-single-active-consumer", false},
};

amqp::FieldValue toFieldValue(const Json& v) { return amqp::FieldValue::fromJson(v); }

bool appliesToQueue(const Policy& p, QueueType type) {
    if (p.applyTo == "all" || p.applyTo == "queues") return true;
    if (p.applyTo == "classic_queues") return type == QueueType::Classic;
    if (p.applyTo == "quorum_queues") return type == QueueType::Quorum;
    if (p.applyTo == "streams") return type == QueueType::Stream;
    return false;
}

bool appliesToExchange(const Policy& p, QueueType) { return p.applyTo == "all" || p.applyTo == "exchanges"; }

}  // namespace

Json Policy::toJson() const {
    Json j = Json::object();
    j["name"] = name;
    j["vhost"] = vhost;
    j["pattern"] = pattern;
    j["apply-to"] = applyTo;
    j["priority"] = priority;
    j["definition"] = definition;
    return j;
}

std::optional<Policy> Policy::fromJson(const Json& j, std::string* error) {
    Policy p;
    p.name = j["name"].asString();
    p.vhost = j["vhost"].asString("/");
    p.pattern = j["pattern"].asString();
    p.applyTo = j["apply-to"].asString("all");
    p.priority = static_cast<int>(j["priority"].asInt(0));
    p.definition = j["definition"];
    if (p.name.empty()) {
        if (error) *error = "policy name is required";
        return std::nullopt;
    }
    if (!p.definition.isObject() || p.definition.size() == 0) {
        if (error) *error = "policy definition must be a non-empty object";
        return std::nullopt;
    }
    static const char* kApplyTo[] = {"all", "queues", "exchanges", "classic_queues", "quorum_queues", "streams"};
    if (std::find(std::begin(kApplyTo), std::end(kApplyTo), p.applyTo) == std::end(kApplyTo)) {
        if (error) *error = "invalid apply-to value '" + p.applyTo + "'";
        return std::nullopt;
    }
    return p;
}

bool PolicyStore::set(const Policy& p, std::string* error) {
    std::regex re;
    try {
        re = std::regex(p.pattern, std::regex::ECMAScript | std::regex::optimize);
    } catch (const std::regex_error& e) {
        if (error) *error = std::string("invalid pattern: ") + e.what();
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    policies_[{p.vhost, p.name}] = {p, std::move(re)};
    return true;
}

bool PolicyStore::clear(const std::string& vhost, const std::string& name) {
    std::lock_guard<std::mutex> lock(mu_);
    return policies_.erase({vhost, name}) > 0;
}

void PolicyStore::clearVhost(const std::string& vhost) {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto it = policies_.begin(); it != policies_.end();) {
        if (it->first.first == vhost) it = policies_.erase(it);
        else ++it;
    }
}

std::vector<Policy> PolicyStore::list(const std::string& vhost) const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<Policy> out;
    for (const auto& [key, entry] : policies_) {
        if (vhost.empty() || key.first == vhost) out.push_back(entry.first);
    }
    return out;
}

std::optional<Policy> PolicyStore::get(const std::string& vhost, const std::string& name) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = policies_.find({vhost, name});
    if (it == policies_.end()) return std::nullopt;
    return it->second.first;
}

std::optional<Policy> PolicyStore::bestMatch(const std::string& vhost, const std::string& name,
                                             bool (*applies)(const Policy&, QueueType), QueueType type) const {
    std::lock_guard<std::mutex> lock(mu_);
    const Policy* best = nullptr;
    for (const auto& [key, entry] : policies_) {
        if (key.first != vhost) continue;
        const Policy& p = entry.first;
        if (!applies(p, type)) continue;
        if (!std::regex_search(name, entry.second)) continue;
        if (!best || p.priority > best->priority) best = &p;
    }
    if (!best) return std::nullopt;
    return *best;
}

std::optional<Policy> PolicyStore::matchQueue(const std::string& vhost, const std::string& name,
                                              QueueType type) const {
    return bestMatch(vhost, name, appliesToQueue, type);
}

std::optional<Policy> PolicyStore::matchExchange(const std::string& vhost, const std::string& name) const {
    return bestMatch(vhost, name, appliesToExchange, QueueType::Classic);
}

amqp::FieldTable PolicyStore::effectiveQueueArguments(const amqp::FieldTable& declared, const Policy* policy) {
    amqp::FieldTable out = declared;
    if (!policy) return out;
    for (const auto& m : kQueueKeys) {
        const Json* v = policy->definition.find(m.policyKey);
        if (!v) continue;
        const amqp::FieldValue* existing = declared.get(m.argument);
        if (!existing) {
            out.set(m.argument, toFieldValue(*v));
            continue;
        }
        if (m.numericMinimum) {
            auto a = existing->asInt();
            if (a && v->isNumber() && v->asInt() < *a) out.set(m.argument, toFieldValue(*v));
        }
    }
    return out;
}

amqp::FieldTable PolicyStore::effectiveExchangeArguments(const amqp::FieldTable& declared, const Policy* policy) {
    amqp::FieldTable out = declared;
    if (!policy) return out;
    if (const Json* ae = policy->definition.find("alternate-exchange")) {
        if (!declared.has("alternate-exchange")) out.set("alternate-exchange", toFieldValue(*ae));
    }
    return out;
}

}  // namespace tq::broker
