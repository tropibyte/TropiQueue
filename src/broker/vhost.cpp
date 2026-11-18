// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/vhost.h"

#include <algorithm>
#include <mutex>

#include "amqp/constants.h"
#include "broker/broker.h"
#include "broker/classic_queue.h"
#include "broker/events.h"
#include "broker/policy.h"
#include "common/clock.h"
#include "common/logging.h"
#include "common/strings.h"

namespace tq::broker {

using namespace tq::amqp;

Json OpResult::toJson() const {
    Json j = Json::object();
    j["code"] = static_cast<int>(code);
    if (!text.empty()) j["text"] = text;
    return j;
}

OpResult OpResult::fromJson(const Json& j) {
    OpResult r;
    r.code = static_cast<uint16_t>(j["code"].asInt(0));
    r.text = j["text"].asString();
    return r;
}

Json VhostLimits::toJson() const {
    Json j = Json::object();
    if (maxConnections >= 0) j["max-connections"] = maxConnections;
    if (maxQueues >= 0) j["max-queues"] = maxQueues;
    return j;
}

VhostLimits VhostLimits::fromJson(const Json& j) {
    VhostLimits l;
    l.maxConnections = j["max-connections"].asInt(-1);
    l.maxQueues = j["max-queues"].asInt(-1);
    return l;
}

VirtualHost::VirtualHost(std::string name, Broker& broker) : name_(std::move(name)), broker_(broker) {}

VirtualHost::~VirtualHost() = default;

void VirtualHost::createDefaultExchanges() {
    struct Def {
        const char* name;
        ExchangeType type;
    };
    const Def defs[] = {
        {"", ExchangeType::Direct},          {"amq.direct", ExchangeType::Direct},
        {"amq.fanout", ExchangeType::Fanout}, {"amq.topic", ExchangeType::Topic},
        {"amq.headers", ExchangeType::Headers}, {"amq.match", ExchangeType::Headers},
        {kEventExchange, ExchangeType::Topic}, {kTraceExchange, ExchangeType::Topic},
    };
    std::unique_lock lock(mu_);
    for (const auto& d : defs) {
        if (exchanges_.count(d.name)) continue;
        ExchangeDef ed;
        ed.name = d.name;
        ed.vhost = name_;
        ed.type = d.type;
        ed.durable = true;
        ed.internal = std::string(d.name) == kEventExchange || std::string(d.name) == kTraceExchange;
        exchanges_[d.name] = makeExchange(ed);
    }
}

// ------------------------------------------------------------------ exchanges

OpResult VirtualHost::declareExchange(const ExchangeDef& def, bool passive) {
    std::unique_lock lock(mu_);
    auto it = exchanges_.find(def.name);
    if (passive) {
        if (it == exchanges_.end())
            return OpResult::fail(kNotFound, "NOT_FOUND - no exchange '" + def.name + "' in vhost '" + name_ + "'");
        return OpResult::success();
    }
    if (it != exchanges_.end()) {
        const ExchangeDef& cur = it->second->def();
        if (cur.type != def.type || cur.durable != def.durable || cur.autoDelete != def.autoDelete ||
            cur.internal != def.internal) {
            return OpResult::fail(kPreconditionFailed, "PRECONDITION_FAILED - inequivalent arg for exchange '" +
                                                           def.name + "' in vhost '" + name_ + "'");
        }
        return OpResult::success();
    }
    if (def.name.empty() || str::startsWith(def.name, "amq.")) {
        return OpResult::fail(kAccessRefused, "ACCESS_REFUSED - exchange name '" + def.name +
                                                  "' contains reserved prefix 'amq.*'");
    }
    ExchangeDef effective = def;
    effective.vhost = name_;
    auto policy = broker_.policies().matchExchange(name_, def.name);
    effective.arguments = PolicyStore::effectiveExchangeArguments(def.arguments, policy ? &*policy : nullptr);
    exchanges_[def.name] = makeExchange(effective);
    broker_.events().emit(name_, "exchange.created",
                          eventDetails({{"name", def.name}, {"type", exchangeTypeName(def.type)},
                                        {"durable", def.durable ? "true" : "false"}}));
    return OpResult::success();
}

OpResult VirtualHost::deleteExchange(const std::string& name, bool ifUnused) {
    std::unique_lock lock(mu_);
    auto it = exchanges_.find(name);
    if (it == exchanges_.end())
        return OpResult::fail(kNotFound, "NOT_FOUND - no exchange '" + name + "' in vhost '" + name_ + "'");
    if (ifUnused && it->second->hasBindings())
        return OpResult::fail(kPreconditionFailed, "PRECONDITION_FAILED - exchange '" + name + "' in use");
    // Remove bindings where this exchange is the destination.
    for (auto& [n, ex] : exchanges_) {
        if (n != name) ex->removeBindingsTo(DestinationKind::Exchange, name);
    }
    exchanges_.erase(it);
    broker_.events().emit(name_, "exchange.deleted", eventDetails({{"name", name}}));
    return OpResult::success();
}

std::optional<ExchangeDef> VirtualHost::exchangeDef(const std::string& name) const {
    std::shared_lock lock(mu_);
    auto it = exchanges_.find(name);
    if (it == exchanges_.end()) return std::nullopt;
    return it->second->def();
}

std::vector<ExchangeDef> VirtualHost::exchanges() const {
    std::shared_lock lock(mu_);
    std::vector<ExchangeDef> out;
    for (const auto& [n, ex] : exchanges_) out.push_back(ex->def());
    return out;
}

// --------------------------------------------------------------------- queues

OpResult VirtualHost::declareQueue(QueueDef def, bool passive, QueuePtr* out) {
    {
        std::unique_lock lock(mu_);
        auto it = queues_.find(def.name);
        if (it != queues_.end()) {
            QueuePtr q = it->second;
            q->touch(wallClockMs());
            if (out) *out = q;
            return OpResult::success();
        }
        if (passive)
            return OpResult::fail(kNotFound, "NOT_FOUND - no queue '" + def.name + "' in vhost '" + name_ + "'");
    }
    if (limits_.maxQueues >= 0 && static_cast<int64_t>(queueCount()) >= limits_.maxQueues) {
        return OpResult::fail(kPreconditionFailed,
                              "PRECONDITION_FAILED - cannot declare queue '" + def.name + "': queue limit in vhost '" +
                                  name_ + "' (" + std::to_string(limits_.maxQueues) + ") is reached");
    }
    def.vhost = name_;
    if (def.node.empty()) def.node = broker_.nodeTag();
    QueuePtr q = broker_.createQueueInstance(def);
    if (!q) return OpResult::fail(kPreconditionFailed, "PRECONDITION_FAILED - cannot create queue '" + def.name + "'");
    auto policy = broker_.policies().matchQueue(name_, def.name, def.type);
    q->setEffectiveArguments(PolicyStore::effectiveQueueArguments(def.arguments, policy ? &*policy : nullptr));
    if (auto* cq = dynamic_cast<ClassicQueue*>(q.get())) cq->reconfigure();
    q->setDeadLetterSink(this);
    q->recover();

    std::unique_lock lock(mu_);
    auto [it, inserted] = queues_.emplace(def.name, q);
    if (!inserted) {
        if (out) *out = it->second;
        return OpResult::success();
    }
    // Every queue is implicitly bound to the default exchange.
    if (out) *out = q;
    broker_.events().emit(name_, "queue.created",
                          eventDetails({{"name", def.name}, {"type", queueTypeName(def.type)},
                                        {"durable", def.durable ? "true" : "false"},
                                        {"exclusive", def.exclusive ? "true" : "false"}}));
    return OpResult::success();
}

void VirtualHost::removeQueueLocked(const QueuePtr& q) {
    for (auto& [n, ex] : exchanges_) ex->removeBindingsTo(DestinationKind::Queue, q->name());
    queues_.erase(q->name());
}

OpResult VirtualHost::deleteQueue(const std::string& name, bool ifUnused, bool ifEmpty, uint32_t* messageCount) {
    QueuePtr q;
    {
        std::unique_lock lock(mu_);
        auto it = queues_.find(name);
        if (it == queues_.end())
            return OpResult::fail(kNotFound, "NOT_FOUND - no queue '" + name + "' in vhost '" + name_ + "'");
        q = it->second;
        if (ifUnused && q->consumerCount() > 0)
            return OpResult::fail(kPreconditionFailed, "PRECONDITION_FAILED - queue '" + name + "' in use");
        if (ifEmpty && q->messageCount() > 0)
            return OpResult::fail(kPreconditionFailed, "PRECONDITION_FAILED - queue '" + name + "' not empty");
        if (messageCount) *messageCount = static_cast<uint32_t>(q->messageCount());
        removeQueueLocked(q);
    }
    q->deleted();
    broker_.events().emit(name_, "queue.deleted", eventDetails({{"name", name}}));
    return OpResult::success();
}

QueuePtr VirtualHost::findQueue(const std::string& name) const {
    std::shared_lock lock(mu_);
    auto it = queues_.find(name);
    return it == queues_.end() ? nullptr : it->second;
}

std::vector<QueuePtr> VirtualHost::queues() const {
    std::shared_lock lock(mu_);
    std::vector<QueuePtr> out;
    for (const auto& [n, q] : queues_) out.push_back(q);
    return out;
}

size_t VirtualHost::queueCount() const {
    std::shared_lock lock(mu_);
    return queues_.size();
}

// ------------------------------------------------------------------- bindings

OpResult VirtualHost::bind(const Binding& b) {
    std::unique_lock lock(mu_);
    auto src = exchanges_.find(b.source);
    if (src == exchanges_.end())
        return OpResult::fail(kNotFound, "NOT_FOUND - no exchange '" + b.source + "' in vhost '" + name_ + "'");
    if (b.source.empty())
        return OpResult::fail(kAccessRefused, "ACCESS_REFUSED - operation not permitted on the default exchange");
    if (b.kind == DestinationKind::Queue) {
        if (!queues_.count(b.destination))
            return OpResult::fail(kNotFound, "NOT_FOUND - no queue '" + b.destination + "' in vhost '" + name_ + "'");
    } else if (!exchanges_.count(b.destination)) {
        return OpResult::fail(kNotFound, "NOT_FOUND - no exchange '" + b.destination + "' in vhost '" + name_ + "'");
    }
    src->second->addBinding(b);
    broker_.events().emit(name_, "binding.created",
                          eventDetails({{"source_name", b.source},
                                        {"destination_name", b.destination},
                                        {"destination_kind", b.kind == DestinationKind::Queue ? "queue" : "exchange"},
                                        {"routing_key", b.routingKey}}));
    return OpResult::success();
}

OpResult VirtualHost::unbind(const Binding& b) {
    std::unique_lock lock(mu_);
    auto src = exchanges_.find(b.source);
    if (src == exchanges_.end())
        return OpResult::fail(kNotFound, "NOT_FOUND - no exchange '" + b.source + "' in vhost '" + name_ + "'");
    if (b.source.empty())
        return OpResult::fail(kAccessRefused, "ACCESS_REFUSED - operation not permitted on the default exchange");
    src->second->removeBinding(b);
    if (src->second->def().autoDelete && !src->second->hasBindings()) exchanges_.erase(src);
    broker_.events().emit(name_, "binding.deleted",
                          eventDetails({{"source_name", b.source},
                                        {"destination_name", b.destination},
                                        {"destination_kind", b.kind == DestinationKind::Queue ? "queue" : "exchange"},
                                        {"routing_key", b.routingKey}}));
    return OpResult::success();
}

std::vector<Binding> VirtualHost::bindings() const {
    std::shared_lock lock(mu_);
    std::vector<Binding> out;
    for (const auto& [n, ex] : exchanges_) {
        for (const auto& b : ex->bindings()) out.push_back(b);
    }
    for (const auto& [n, q] : queues_) {
        Binding implicit;
        implicit.source = "";
        implicit.destination = n;
        implicit.routingKey = n;
        out.push_back(implicit);
    }
    return out;
}

// -------------------------------------------------------------------- routing

void VirtualHost::addUnique(std::vector<QueuePtr>& out, const QueuePtr& q) const {
    if (std::find(out.begin(), out.end(), q) == out.end()) out.push_back(q);
}

void VirtualHost::routeExchange(const Exchange& ex, const Message& msg, const std::string& routingKey,
                                std::vector<QueuePtr>& out) const {
    std::vector<const Binding*> matched;
    ex.route(msg, routingKey, matched);
    for (const Binding* b : matched) {
        if (b->kind == DestinationKind::Queue) {
            auto q = queues_.find(b->destination);
            if (q != queues_.end()) addUnique(out, q->second);
        } else {
            auto dest = exchanges_.find(b->destination);
            if (dest != exchanges_.end()) routeExchange(*dest->second, msg, routingKey, out);
        }
    }
}

RouteResult VirtualHost::route(const std::string& exchange, const MessagePtr& msg) {
    RouteResult result;
    std::shared_lock lock(mu_);
    auto it = exchanges_.find(exchange);
    if (it == exchanges_.end()) {
        result.exchangeFound = false;
        return result;
    }
    if (exchange.empty()) {
        auto q = queues_.find(msg->routingKey);
        if (q != queues_.end()) result.queues.push_back(q->second);
        for (const auto& extra : Exchange::extraRoutingKeys(*msg)) {
            auto e = queues_.find(extra);
            if (e != queues_.end()) addUnique(result.queues, e->second);
        }
        return result;
    }
    const Exchange* ex = it->second.get();
    if (ex->type() == ExchangeType::Delayed && msg->props.headers) {
        auto delay = msg->props.headers->getInt("x-delay");
        if (delay && *delay > 0) {
            lock.unlock();
            scheduleDelayed(exchange, msg, *delay);
            result.delayed = true;
            return result;
        }
    }
    routeExchange(*ex, *msg, msg->routingKey, result.queues);
    while (result.queues.empty()) {
        auto ae = ex->alternateExchange();
        if (!ae) break;
        auto aeIt = exchanges_.find(*ae);
        if (aeIt == exchanges_.end()) break;
        ex = aeIt->second.get();
        routeExchange(*ex, *msg, msg->routingKey, result.queues);
    }
    return result;
}

size_t VirtualHost::publish(const std::string& exchange, const MessagePtr& msg, bool* routed) {
    RouteResult r = route(exchange, msg);
    if (routed) *routed = !r.queues.empty() || r.delayed;
    size_t accepted = 0;
    for (const auto& q : r.queues) {
        if (q->enqueue(msg) == QueueBase::EnqueueResult::Accepted) ++accepted;
    }
    return accepted;
}

void VirtualHost::scheduleDelayed(const std::string& exchange, const MessagePtr& msg, int64_t delayMs) {
    std::weak_ptr<VirtualHost> weak = shared_from_this();
    delayedPending_++;
    broker_.scheduler().scheduleAfter(delayMs, [weak, exchange, msg] {
        auto self = weak.lock();
        if (!self) return;
        self->delayedPending_--;
        auto copy = msg->clone();
        copy->props.mutableHeaders().erase("x-delay");
        std::shared_lock lock(self->mu_);
        auto it = self->exchanges_.find(exchange);
        if (it == self->exchanges_.end()) return;
        std::vector<QueuePtr> targets;
        self->routeExchange(*it->second, *copy, copy->routingKey, targets);
        lock.unlock();
        for (const auto& q : targets) q->enqueue(copy);
    });
}

// ---------------------------------------------------------------- dead letters

void VirtualHost::deadLetter(const QueueBase& from, const MessagePtr& msg, const std::string& reason) {
    const amqp::FieldTable& args = from.effectiveArguments();
    auto dlx = args.getString("x-dead-letter-exchange");
    if (!dlx) return;
    auto dlrk = args.getString("x-dead-letter-routing-key");

    auto copy = msg->clone();
    copy->id = nextMessageId();
    amqp::FieldTable& headers = copy->props.mutableHeaders();
    amqp::FieldArray deaths;
    if (const amqp::FieldValue* existing = headers.get("x-death"); existing && existing->isArray() && existing->array)
        deaths = *existing->array;

    bool updated = false;
    for (size_t i = 0; i < deaths.size(); ++i) {
        auto& d = deaths[i];
        if (!d.isTable() || !d.table) continue;
        if (d.table->getString("queue") == from.name() && d.table->getString("reason") == reason) {
            amqp::FieldTable t = *d.table;
            t.set("count", amqp::FieldValue::int64(t.getInt("count").value_or(0) + 1));
            t.set("time", amqp::FieldValue::timestamp(static_cast<uint64_t>(wallClockMs() / 1000)));
            deaths.erase(deaths.begin() + static_cast<long>(i));
            deaths.insert(deaths.begin(), amqp::FieldValue::fromTable(std::move(t)));
            updated = true;
            break;
        }
    }
    if (!updated) {
        amqp::FieldTable t;
        t.set("count", amqp::FieldValue::int64(1));
        t.set("reason", amqp::FieldValue::longString(reason));
        t.set("queue", amqp::FieldValue::longString(from.name()));
        t.set("time", amqp::FieldValue::timestamp(static_cast<uint64_t>(wallClockMs() / 1000)));
        t.set("exchange", amqp::FieldValue::longString(msg->exchange));
        amqp::FieldArray keys{amqp::FieldValue::longString(msg->routingKey)};
        t.set("routing-keys", amqp::FieldValue::fromArray(std::move(keys)));
        if (msg->props.expiration) t.set("original-expiration", amqp::FieldValue::longString(*msg->props.expiration));
        deaths.insert(deaths.begin(), amqp::FieldValue::fromTable(std::move(t)));
        if (!headers.has("x-first-death-reason")) {
            headers.set("x-first-death-reason", amqp::FieldValue::longString(reason));
            headers.set("x-first-death-queue", amqp::FieldValue::longString(from.name()));
            headers.set("x-first-death-exchange", amqp::FieldValue::longString(msg->exchange));
        }
    }
    headers.set("x-death", amqp::FieldValue::fromArray(std::move(deaths)));
    headers.set("x-last-death-reason", amqp::FieldValue::longString(reason));
    headers.set("x-last-death-queue", amqp::FieldValue::longString(from.name()));
    copy->props.expiration.reset();
    if (dlrk) copy->routingKey = *dlrk;
    copy->exchange = *dlx;
    publish(*dlx, copy);
}

// ------------------------------------------------------------------ lifecycle

void VirtualHost::tick(int64_t nowMs) {
    std::vector<QueuePtr> all = queues();
    std::vector<QueuePtr> expired;
    for (const auto& q : all) {
        q->tick(nowMs);
        if (q->expired(nowMs)) expired.push_back(q);
    }
    for (const auto& q : expired) {
        LOG_INFO("vhost") << "deleting queue '" << q->name() << "' in vhost '" << name_ << "' (x-expires)";
        if (q->durable()) {
            Json cmd = Json::object();
            cmd["op"] = "delete_queue";
            cmd["vhost"] = name_;
            cmd["name"] = q->name();
            broker_.commitMetadata(cmd);
        } else {
            deleteQueue(q->name(), false, false, nullptr);
        }
    }
}

void VirtualHost::deleteExclusiveQueues(uint64_t connectionId) {
    std::vector<QueuePtr> victims;
    {
        std::unique_lock lock(mu_);
        for (const auto& [n, q] : queues_) {
            if (q->exclusive() && q->exclusiveOwner() == connectionId) victims.push_back(q);
        }
        for (const auto& q : victims) removeQueueLocked(q);
    }
    for (const auto& q : victims) q->deleted();
}

void VirtualHost::maybeAutoDelete(const QueuePtr& q) {
    if (!q->autoDelete() || q->consumerCount() > 0) return;
    {
        std::unique_lock lock(mu_);
        auto it = queues_.find(q->name());
        if (it == queues_.end() || it->second != q) return;
        removeQueueLocked(q);
    }
    q->deleted();
}

void VirtualHost::reapplyPolicies() {
    std::shared_lock lock(mu_);
    for (auto& [n, q] : queues_) {
        auto policy = broker_.policies().matchQueue(name_, n, q->type());
        q->setEffectiveArguments(PolicyStore::effectiveQueueArguments(q->def().arguments, policy ? &*policy : nullptr));
        if (auto* cq = dynamic_cast<ClassicQueue*>(q.get())) cq->reconfigure();
    }
}

void VirtualHost::shutdown() {
    std::vector<QueuePtr> all = queues();
    for (const auto& q : all) q->tick(wallClockMs());
}

VhostLimits VirtualHost::limits() const {
    std::shared_lock lock(mu_);
    return limits_;
}

void VirtualHost::setLimits(const VhostLimits& l) {
    std::unique_lock lock(mu_);
    limits_ = l;
}

Json VirtualHost::durableDefinitions() const {
    Json j = Json::object();
    Json exchanges = Json::array();
    Json queues = Json::array();
    Json bindings = Json::array();
    for (const auto& [n, ex] : exchanges_) {
        if (n.empty() || str::startsWith(n, "amq.")) continue;
        if (ex->def().durable) exchanges.push(ex->def().toJson());
    }
    for (const auto& [n, q] : queues_) {
        if (q->durable() && !q->exclusive()) queues.push(q->def().toJson());
    }
    for (const auto& [n, ex] : exchanges_) {
        if (!ex->def().durable) continue;
        for (const auto& b : ex->bindings()) {
            bool destDurable = false;
            if (b.kind == DestinationKind::Queue) {
                auto q = queues_.find(b.destination);
                destDurable = q != queues_.end() && q->second->durable() && !q->second->exclusive();
            } else {
                auto e = exchanges_.find(b.destination);
                destDurable = e != exchanges_.end() && e->second->def().durable;
            }
            if (destDurable) bindings.push(b.toJson(name_));
        }
    }
    j["exchanges"] = exchanges;
    j["queues"] = queues;
    j["bindings"] = bindings;
    j["limits"] = limits_.toJson();
    return j;
}

}  // namespace tq::broker
