// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/queue.h"

#include "common/clock.h"

namespace tq::broker {

const char* queueTypeName(QueueType t) {
    switch (t) {
        case QueueType::Classic: return "classic";
        case QueueType::Quorum: return "quorum";
        case QueueType::Stream: return "stream";
    }
    return "classic";
}

std::optional<QueueType> parseQueueType(const std::string& name) {
    if (name.empty() || name == "classic") return QueueType::Classic;
    if (name == "quorum") return QueueType::Quorum;
    if (name == "stream") return QueueType::Stream;
    return std::nullopt;
}

Json QueueDef::toJson() const {
    Json j = Json::object();
    j["name"] = name;
    j["vhost"] = vhost;
    j["type"] = queueTypeName(type);
    j["durable"] = durable;
    j["exclusive"] = exclusive;
    j["auto_delete"] = autoDelete;
    j["arguments"] = arguments.toJson();
    if (!node.empty()) j["node"] = node;
    if (!replicas.empty()) {
        Json r = Json::array();
        for (int id : replicas) r.push(id);
        j["replicas"] = r;
    }
    return j;
}

std::optional<QueueDef> QueueDef::fromJson(const Json& j) {
    QueueDef d;
    d.name = j["name"].asString();
    if (d.name.empty()) return std::nullopt;
    d.vhost = j["vhost"].asString("/");
    auto t = parseQueueType(j["type"].asString("classic"));
    if (!t) return std::nullopt;
    d.type = *t;
    d.durable = j["durable"].asBool(true);
    d.exclusive = j["exclusive"].asBool(false);
    d.autoDelete = j["auto_delete"].asBool(false);
    d.arguments = amqp::FieldTable::fromJson(j["arguments"]);
    d.node = j["node"].asString();
    for (const auto& r : j["replicas"].asArray()) d.replicas.push_back(static_cast<int>(r.asInt()));
    return d;
}

QueueBase::QueueBase(QueueDef def) : def_(std::move(def)), effectiveArgs_(def_.arguments) {
    lastUsedMs_.store(wallClockMs());
    expiresMs_ = effectiveArgs_.getInt("x-expires").value_or(0);
}

void QueueBase::setEffectiveArguments(amqp::FieldTable args) {
    effectiveArgs_ = std::move(args);
    expiresMs_ = effectiveArgs_.getInt("x-expires").value_or(0);
}

bool QueueBase::expired(int64_t nowMs) const {
    if (expiresMs_ <= 0) return false;
    return nowMs - lastUsedMs_.load() >= expiresMs_;
}

Json QueueBase::info() const {
    Json j = def_.toJson();
    j["effective_arguments"] = effectiveArgs_.toJson();
    j["messages"] = static_cast<int64_t>(messageCount() + unackedCount());
    j["messages_ready"] = static_cast<int64_t>(messageCount());
    j["messages_unacknowledged"] = static_cast<int64_t>(unackedCount());
    j["message_bytes"] = static_cast<int64_t>(messageBytes());
    j["consumers"] = static_cast<int64_t>(consumerCount());
    Json stats = Json::object();
    stats["publish"] = static_cast<int64_t>(counters_.published.load());
    stats["deliver"] = static_cast<int64_t>(counters_.delivered.load());
    stats["ack"] = static_cast<int64_t>(counters_.acked.load());
    stats["redeliver"] = static_cast<int64_t>(counters_.redelivered.load());
    stats["expired"] = static_cast<int64_t>(counters_.expired.load());
    stats["dead_lettered"] = static_cast<int64_t>(counters_.deadLettered.load());
    stats["dropped"] = static_cast<int64_t>(counters_.dropped.load());
    j["message_stats"] = stats;
    return j;
}

}  // namespace tq::broker
