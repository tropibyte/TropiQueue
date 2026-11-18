// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/channel.h"

#include <algorithm>

#include "amqp/constants.h"
#include "broker/broker.h"
#include "broker/broker_metrics.h"
#include "broker/connection.h"
#include "broker/vhost.h"
#include "common/clock.h"
#include "common/logging.h"
#include "common/random.h"
#include "common/strings.h"
#include "security/users.h"

namespace tq::broker {

using namespace tq::amqp;
using security::Access;

// -------------------------------------------------------- DirectReplyRegistry

DirectReplyRegistry& DirectReplyRegistry::instance() {
    static DirectReplyRegistry r;
    return r;
}

std::string DirectReplyRegistry::registerChannel(const std::shared_ptr<Channel>& ch) {
    std::string address = std::string(kDirectReplyTo) + "." + rnd::uniqueName("", 24);
    std::lock_guard<std::mutex> lock(mu_);
    channels_[address] = ch;
    return address;
}

void DirectReplyRegistry::unregister(const std::string& address) {
    std::lock_guard<std::mutex> lock(mu_);
    channels_.erase(address);
}

std::shared_ptr<Channel> DirectReplyRegistry::find(const std::string& address) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = channels_.find(address);
    return it == channels_.end() ? nullptr : it->second.lock();
}

// ------------------------------------------------------------ ChannelConsumer

ChannelConsumer::ChannelConsumer(std::weak_ptr<Channel> channel, std::string tag, std::weak_ptr<QueueBase> queue,
                                 bool noAck, bool exclusive, int priority, uint16_t prefetch, FieldTable args,
                                 uint64_t connectionId)
    : channel_(std::move(channel)),
      tag_(std::move(tag)),
      queue_(std::move(queue)),
      noAck_(noAck),
      exclusive_(exclusive),
      priority_(priority),
      prefetch_(prefetch),
      args_(std::move(args)),
      connectionId_(connectionId) {}

bool ChannelConsumer::hasCredit() const {
    auto ch = channel_.lock();
    return ch && !ch->closing() && ch->consumerHasCredit(*this);
}

void ChannelConsumer::deliver(QueueBase& queue, Delivery delivery) {
    if (auto ch = channel_.lock()) ch->deliver(*this, queue, std::move(delivery));
}

void ChannelConsumer::cancelledByServer(const std::string& reason) {
    if (auto ch = channel_.lock()) ch->consumerCancelled(tag_, reason);
}

// -------------------------------------------------------------------- Channel

Channel::Channel(AmqpConnection& conn, uint16_t id) : conn_(conn), connRef_(conn.shared_from_this()), id_(id) {}

Channel::~Channel() = default;

Broker& Channel::broker() { return conn_.broker(); }
VirtualHost& Channel::vhost() { return *conn_.vhost(); }
const std::string& Channel::user() const { return conn_.user(); }

void Channel::fail(uint16_t code, const std::string& text, const Method& m) const {
    throw AmqpException(code, text, classIdOf(m), methodIdOf(m));
}

void Channel::checkAccess(int access, const std::string& resource, const Method& m) {
    auto a = static_cast<Access>(access);
    if (broker().users().checkResource(user(), conn_.vhostName(), a, resource)) return;
    const char* what = a == Access::Configure ? "configure" : a == Access::Write ? "write" : "read";
    fail(kAccessRefused, std::string("ACCESS_REFUSED - ") + what + " access to '" + resource + "' in vhost '" +
                             conn_.vhostName() + "' refused for user '" + user() + "'",
         m);
}

void Channel::send(const Method& m) { conn_.sendMethod(id_, m); }

QueuePtr Channel::findQueueOrFail(const std::string& name, const Method& m) {
    QueuePtr q = vhost().findQueue(name);
    if (!q) fail(kNotFound, "NOT_FOUND - no queue '" + name + "' in vhost '" + conn_.vhostName() + "'", m);
    return q;
}

void Channel::handleMethod(const Method& m) {
    if (auto* x = methodAs<ExchangeDeclare>(m)) return onExchangeDeclare(*x);
    if (auto* x = methodAs<ExchangeDelete>(m)) return onExchangeDelete(*x);
    if (auto* x = methodAs<ExchangeBind>(m)) return onExchangeBind(*x, true);
    if (auto* x = methodAs<ExchangeUnbind>(m)) return onExchangeUnbind(*x);
    if (auto* x = methodAs<QueueDeclare>(m)) return onQueueDeclare(*x);
    if (auto* x = methodAs<QueueBind>(m)) return onQueueBind(*x);
    if (auto* x = methodAs<QueueUnbind>(m)) return onQueueUnbind(*x);
    if (auto* x = methodAs<QueuePurge>(m)) return onQueuePurge(*x);
    if (auto* x = methodAs<QueueDelete>(m)) return onQueueDelete(*x);
    if (auto* x = methodAs<BasicQos>(m)) return onBasicQos(*x);
    if (auto* x = methodAs<BasicConsume>(m)) return onBasicConsume(*x);
    if (auto* x = methodAs<BasicCancel>(m)) return onBasicCancel(*x);
    if (auto* x = methodAs<BasicGet>(m)) return onBasicGet(*x);
    if (auto* x = methodAs<BasicAck>(m)) return onBasicAck(*x);
    if (auto* x = methodAs<BasicNack>(m)) return onBasicNack(x->deliveryTag, x->multiple, x->requeue);
    if (auto* x = methodAs<BasicReject>(m)) return onBasicNack(x->deliveryTag, false, x->requeue);
    if (auto* x = methodAs<BasicRecover>(m)) {
        onBasicRecover(x->requeue);
        send(BasicRecoverOk{});
        return;
    }
    if (auto* x = methodAs<BasicRecoverAsync>(m)) return onBasicRecover(x->requeue);
    if (auto* x = methodAs<ConfirmSelect>(m)) return onConfirmSelect(*x);
    if (methodAs<TxSelect>(m)) return onTxSelect();
    if (methodAs<TxCommit>(m)) return onTxCommit();
    if (methodAs<TxRollback>(m)) return onTxRollback();
    if (auto* x = methodAs<BasicPublish>(m)) {
        std::string resource = x->exchange.empty() ? "amq.default" : x->exchange;
        checkAccess(static_cast<int>(Access::Write), resource, m);
        auto def = vhost().exchangeDef(x->exchange);
        if (!def) fail(kNotFound, "NOT_FOUND - no exchange '" + x->exchange + "' in vhost '" + conn_.vhostName() + "'", m);
        if (def->internal)
            fail(kAccessRefused, "ACCESS_REFUSED - cannot publish to internal exchange '" + x->exchange + "'", m);
        if (x->immediate) throw AmqpException(kNotImplemented, "NOT_IMPLEMENTED - immediate=true", 60, 40);
        pendingPublish_ = *x;
        pendingProps_.reset();
        pendingBody_.clear();
        pendingBodySize_ = 0;
        headerReceived_ = false;
        return;
    }
    throw AmqpException(kCommandInvalid, "COMMAND_INVALID - unexpected method " + methodName(m), classIdOf(m),
                        methodIdOf(m));
}

// ------------------------------------------------------------------ exchanges

void Channel::onExchangeDeclare(const ExchangeDeclare& m) {
    auto type = parseExchangeType(m.type);
    if (!type && !m.passive)
        throw AmqpException(kCommandInvalid, "COMMAND_INVALID - unknown exchange type '" + m.type + "'", 40, 10);
    if (!m.passive) checkAccess(static_cast<int>(Access::Configure), m.exchange, Method(m));
    ExchangeDef def;
    def.name = m.exchange;
    def.vhost = conn_.vhostName();
    def.type = type.value_or(ExchangeType::Direct);
    def.durable = m.durable;
    def.autoDelete = m.autoDelete;
    def.internal = m.internal;
    def.arguments = m.arguments;
    OpResult r;
    if (m.durable && !m.passive && !vhost().exchangeDef(m.exchange)) {
        Json cmd = Json::object();
        cmd["op"] = "declare_exchange";
        cmd["def"] = def.toJson();
        r = OpResult::fromJson(broker().commitMetadata(cmd));
    } else {
        r = vhost().declareExchange(def, m.passive);
    }
    if (!r.ok()) fail(r.code, r.text, Method(m));
    if (!m.noWait) send(ExchangeDeclareOk{});
}

void Channel::onExchangeDelete(const ExchangeDelete& m) {
    checkAccess(static_cast<int>(Access::Configure), m.exchange, Method(m));
    auto def = vhost().exchangeDef(m.exchange);
    if (!def) {
        if (!m.noWait) send(ExchangeDeleteOk{});
        return;
    }
    OpResult r;
    if (def->durable) {
        Json cmd = Json::object();
        cmd["op"] = "delete_exchange";
        cmd["vhost"] = conn_.vhostName();
        cmd["name"] = m.exchange;
        cmd["if_unused"] = m.ifUnused;
        r = OpResult::fromJson(broker().commitMetadata(cmd));
    } else {
        r = vhost().deleteExchange(m.exchange, m.ifUnused);
    }
    if (!r.ok()) fail(r.code, r.text, Method(m));
    if (!m.noWait) send(ExchangeDeleteOk{});
}

void Channel::onExchangeBind(const ExchangeBind& m, bool) {
    checkAccess(static_cast<int>(Access::Write), m.destination, Method(m));
    checkAccess(static_cast<int>(Access::Read), m.source, Method(m));
    Binding b;
    b.source = m.source;
    b.kind = DestinationKind::Exchange;
    b.destination = m.destination;
    b.routingKey = m.routingKey;
    b.arguments = m.arguments;
    auto src = vhost().exchangeDef(m.source);
    auto dst = vhost().exchangeDef(m.destination);
    OpResult r;
    if (src && dst && src->durable && dst->durable) {
        Json cmd = Json::object();
        cmd["op"] = "bind";
        cmd["vhost"] = conn_.vhostName();
        cmd["binding"] = b.toJson(conn_.vhostName());
        r = OpResult::fromJson(broker().commitMetadata(cmd));
    } else {
        r = vhost().bind(b);
    }
    if (!r.ok()) fail(r.code, r.text, Method(m));
    if (!m.noWait) send(ExchangeBindOk{});
}

void Channel::onExchangeUnbind(const ExchangeUnbind& m) {
    checkAccess(static_cast<int>(Access::Write), m.destination, Method(m));
    checkAccess(static_cast<int>(Access::Read), m.source, Method(m));
    Binding b;
    b.source = m.source;
    b.kind = DestinationKind::Exchange;
    b.destination = m.destination;
    b.routingKey = m.routingKey;
    b.arguments = m.arguments;
    auto src = vhost().exchangeDef(m.source);
    auto dst = vhost().exchangeDef(m.destination);
    OpResult r;
    if (src && dst && src->durable && dst->durable) {
        Json cmd = Json::object();
        cmd["op"] = "unbind";
        cmd["vhost"] = conn_.vhostName();
        cmd["binding"] = b.toJson(conn_.vhostName());
        r = OpResult::fromJson(broker().commitMetadata(cmd));
    } else {
        r = vhost().unbind(b);
    }
    if (!r.ok()) fail(r.code, r.text, Method(m));
    if (!m.noWait) send(ExchangeUnbindOk{});
}

// --------------------------------------------------------------------- queues

void Channel::validateQueueArguments(const FieldTable& args, const Method& m) {
    if (auto t = args.getString("x-queue-type")) {
        if (!parseQueueType(*t)) fail(kPreconditionFailed, "PRECONDITION_FAILED - invalid x-queue-type '" + *t + "'", m);
    }
    auto checkNonNegative = [&](const char* name) {
        const FieldValue* v = args.get(name);
        if (!v) return;
        auto i = v->asInt();
        if (!i || *i < 0)
            fail(kPreconditionFailed, std::string("PRECONDITION_FAILED - invalid arg '") + name + "'", m);
    };
    checkNonNegative("x-message-ttl");
    checkNonNegative("x-max-length");
    checkNonNegative("x-max-length-bytes");
    checkNonNegative("x-max-priority");
    checkNonNegative("x-delivery-limit");
    if (const FieldValue* v = args.get("x-expires")) {
        auto i = v->asInt();
        if (!i || *i <= 0) fail(kPreconditionFailed, "PRECONDITION_FAILED - invalid arg 'x-expires'", m);
    }
    if (auto o = args.getString("x-overflow")) {
        if (*o != "drop-head" && *o != "reject-publish" && *o != "reject-publish-dlx")
            fail(kPreconditionFailed, "PRECONDITION_FAILED - invalid arg 'x-overflow'", m);
    }
}

void Channel::onQueueDeclare(const QueueDeclare& m) {
    std::string name = m.queue;
    if (name.empty()) {
        if (m.passive) fail(kNotFound, "NOT_FOUND - no queue '' in vhost '" + conn_.vhostName() + "'", Method(m));
        name = rnd::uniqueName("amq.gen-");
    } else if (str::startsWith(name, "amq.") && !m.passive) {
        fail(kAccessRefused, "ACCESS_REFUSED - queue name '" + name + "' contains reserved prefix 'amq.*'", Method(m));
    }
    if (!m.passive) checkAccess(static_cast<int>(Access::Configure), name, Method(m));
    validateQueueArguments(m.arguments, Method(m));

    QueueDef def;
    def.name = name;
    def.vhost = conn_.vhostName();
    def.type = parseQueueType(m.arguments.getString("x-queue-type").value_or("classic")).value_or(QueueType::Classic);
    def.durable = m.durable;
    def.exclusive = m.exclusive;
    def.autoDelete = m.autoDelete;
    def.arguments = m.arguments;
    def.exclusiveOwner = m.exclusive ? conn_.id() : 0;
    if (def.type != QueueType::Classic && (!def.durable || def.exclusive || def.autoDelete)) {
        fail(kPreconditionFailed,
             std::string("PRECONDITION_FAILED - ") + queueTypeName(def.type) +
                 " queues must be durable, non-exclusive and non-auto-delete",
             Method(m));
    }

    QueuePtr existing = vhost().findQueue(name);
    if (existing && existing->exclusive() && existing->exclusiveOwner() != conn_.id()) {
        fail(kResourceLocked, "RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '" + name +
                                  "' in vhost '" + conn_.vhostName() + "'",
             Method(m));
    }

    QueuePtr q;
    OpResult r;
    if (!existing && !m.passive && def.durable && !def.exclusive) {
        Json cmd = Json::object();
        cmd["op"] = "declare_queue";
        cmd["def"] = def.toJson();
        r = OpResult::fromJson(broker().commitMetadata(cmd));
        q = vhost().findQueue(name);
        if (r.ok() && !q) r = OpResult::fail(kInternalError, "INTERNAL_ERROR - queue declaration was not applied");
    } else {
        r = vhost().declareQueue(def, m.passive, &q);
    }
    if (!r.ok()) fail(r.code, r.text, Method(m));
    if (!m.noWait) {
        QueueDeclareOk ok;
        ok.queue = name;
        ok.messageCount = static_cast<uint32_t>(q->messageCount());
        ok.consumerCount = static_cast<uint32_t>(q->consumerCount());
        send(ok);
    }
}

void Channel::onQueueBind(const QueueBind& m) {
    checkAccess(static_cast<int>(Access::Write), m.queue, Method(m));
    std::string exchange = m.exchange;
    checkAccess(static_cast<int>(Access::Read), exchange, Method(m));
    Binding b;
    b.source = exchange;
    b.kind = DestinationKind::Queue;
    b.destination = m.queue;
    b.routingKey = m.routingKey;
    b.arguments = m.arguments;
    QueuePtr q = findQueueOrFail(m.queue, Method(m));
    auto ex = vhost().exchangeDef(exchange);
    OpResult r;
    if (ex && ex->durable && q->durable() && !q->exclusive()) {
        Json cmd = Json::object();
        cmd["op"] = "bind";
        cmd["vhost"] = conn_.vhostName();
        cmd["binding"] = b.toJson(conn_.vhostName());
        r = OpResult::fromJson(broker().commitMetadata(cmd));
    } else {
        r = vhost().bind(b);
    }
    if (!r.ok()) fail(r.code, r.text, Method(m));
    if (!m.noWait) send(QueueBindOk{});
}

void Channel::onQueueUnbind(const QueueUnbind& m) {
    checkAccess(static_cast<int>(Access::Write), m.queue, Method(m));
    checkAccess(static_cast<int>(Access::Read), m.exchange, Method(m));
    Binding b;
    b.source = m.exchange;
    b.kind = DestinationKind::Queue;
    b.destination = m.queue;
    b.routingKey = m.routingKey;
    b.arguments = m.arguments;
    QueuePtr q = vhost().findQueue(m.queue);
    auto ex = vhost().exchangeDef(m.exchange);
    OpResult r;
    if (q && ex && ex->durable && q->durable() && !q->exclusive()) {
        Json cmd = Json::object();
        cmd["op"] = "unbind";
        cmd["vhost"] = conn_.vhostName();
        cmd["binding"] = b.toJson(conn_.vhostName());
        r = OpResult::fromJson(broker().commitMetadata(cmd));
    } else {
        r = vhost().unbind(b);
    }
    if (!r.ok()) fail(r.code, r.text, Method(m));
    send(QueueUnbindOk{});
}

void Channel::onQueuePurge(const QueuePurge& m) {
    checkAccess(static_cast<int>(Access::Read), m.queue, Method(m));
    QueuePtr q = findQueueOrFail(m.queue, Method(m));
    uint32_t n = q->purge();
    if (!m.noWait) send(QueuePurgeOk{n});
}

void Channel::onQueueDelete(const QueueDelete& m) {
    checkAccess(static_cast<int>(Access::Configure), m.queue, Method(m));
    QueuePtr q = vhost().findQueue(m.queue);
    if (!q) {
        if (!m.noWait) send(QueueDeleteOk{0});
        return;
    }
    if (q->exclusive() && q->exclusiveOwner() != conn_.id()) {
        fail(kResourceLocked, "RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '" + m.queue + "'",
             Method(m));
    }
    uint32_t count = 0;
    OpResult r;
    if (q->durable() && !q->exclusive()) {
        Json cmd = Json::object();
        cmd["op"] = "delete_queue";
        cmd["vhost"] = conn_.vhostName();
        cmd["name"] = m.queue;
        cmd["if_unused"] = m.ifUnused;
        cmd["if_empty"] = m.ifEmpty;
        Json res = broker().commitMetadata(cmd);
        r = OpResult::fromJson(res);
        count = static_cast<uint32_t>(res["message_count"].asInt(0));
    } else {
        r = vhost().deleteQueue(m.queue, m.ifUnused, m.ifEmpty, &count);
    }
    if (!r.ok()) fail(r.code, r.text, Method(m));
    if (!m.noWait) send(QueueDeleteOk{count});
}

// ---------------------------------------------------------------------- basic

void Channel::onBasicQos(const BasicQos& m) {
    if (m.prefetchSize != 0)
        throw AmqpException(kNotImplemented, "NOT_IMPLEMENTED - prefetch_size!=0 (" + std::to_string(m.prefetchSize) + ")",
                            60, 10);
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (m.global) globalPrefetch_ = m.prefetchCount;
        else prefetchCount_ = m.prefetchCount;
    }
    send(BasicQosOk{});
    std::vector<QueuePtr> queues;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto& [tag, c] : consumers_) {
            if (auto q = c->queue()) queues.push_back(q);
        }
    }
    for (auto& q : queues) q->creditAvailable();
}

void Channel::onBasicConsume(const BasicConsume& m) {
    std::string tag = m.consumerTag.empty() ? rnd::uniqueName("amq.ctag-") : m.consumerTag;
    if (m.queue == kDirectReplyTo) {
        if (!m.noAck)
            fail(kPreconditionFailed, "PRECONDITION_FAILED - reply consumer cannot acknowledge", Method(m));
        if (!replyToAddress_.empty())
            fail(kPreconditionFailed, "PRECONDITION_FAILED - reply consumer already set", Method(m));
        replyToAddress_ = DirectReplyRegistry::instance().registerChannel(shared_from_this());
        replyConsumerTag_ = tag;
        if (!m.noWait) send(BasicConsumeOk{tag});
        return;
    }
    checkAccess(static_cast<int>(Access::Read), m.queue, Method(m));
    QueuePtr q = findQueueOrFail(m.queue, Method(m));
    int priority = static_cast<int>(m.arguments.getInt("x-priority").value_or(0));
    std::shared_ptr<ChannelConsumer> consumer;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (consumers_.count(tag))
            throw AmqpException(kNotAllowed, "NOT_ALLOWED - attempt to reuse consumer tag '" + tag + "'", 60, 20);
        consumer = std::make_shared<ChannelConsumer>(weak_from_this(), tag, q, m.noAck, m.exclusive, priority,
                                                     prefetchCount_, m.arguments, conn_.id());
        consumers_[tag] = consumer;
    }
    if (!m.noWait) send(BasicConsumeOk{tag});
    std::string error;
    if (!q->addConsumer(consumer, &error)) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            consumers_.erase(tag);
        }
        fail(kAccessRefused, "ACCESS_REFUSED - " + error, Method(m));
    }
    broker().events().emit(conn_.vhostName(), "consumer.created",
                           eventDetails({{"consumer_tag", tag}, {"queue", q->name()}, {"channel", std::to_string(id_)},
                                         {"connection", conn_.name()}, {"ack_required", m.noAck ? "false" : "true"}}));
}

void Channel::onBasicCancel(const BasicCancel& m) {
    if (!replyConsumerTag_.empty() && m.consumerTag == replyConsumerTag_) {
        DirectReplyRegistry::instance().unregister(replyToAddress_);
        replyToAddress_.clear();
        replyConsumerTag_.clear();
        if (!m.noWait) send(BasicCancelOk{m.consumerTag});
        return;
    }
    std::shared_ptr<ChannelConsumer> consumer;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = consumers_.find(m.consumerTag);
        if (it != consumers_.end()) {
            consumer = it->second;
            consumers_.erase(it);
        }
    }
    if (consumer) {
        if (auto q = consumer->queue()) {
            q->removeConsumer(consumer->tag(), conn_.id());
            broker().events().emit(conn_.vhostName(), "consumer.deleted",
                                   eventDetails({{"consumer_tag", consumer->tag()}, {"queue", q->name()},
                                                 {"connection", conn_.name()}}));
            vhost().maybeAutoDelete(q);
        }
    }
    if (!m.noWait) send(BasicCancelOk{m.consumerTag});
}

void Channel::onBasicGet(const BasicGet& m) {
    checkAccess(static_cast<int>(Access::Read), m.queue, Method(m));
    QueuePtr q = findQueueOrFail(m.queue, Method(m));
    auto d = q->basicGet(m.noAck);
    if (!d) {
        send(BasicGetEmpty{});
        return;
    }
    auto conn = connRef_.lock();
    if (!conn) return;
    BasicGetOk ok;
    {
        std::lock_guard<std::mutex> lock(mu_);
        ok.deliveryTag = nextDeliveryTag_++;
        if (!m.noAck) {
            Unacked u;
            u.queue = q.get();
            u.queueTag = d->queueTag;
            u.deliveredAtMs = wallClockMs();
            unacked_[ok.deliveryTag] = u;
        }
        deliveredCount_++;
    }
    ok.redelivered = d->redelivered;
    ok.exchange = d->msg->exchange;
    ok.routingKey = d->msg->routingKey;
    ok.messageCount = d->remaining;
    conn->sendRaw(encodeContentFrames(id_, ok, d->msg->props, d->msg->body, conn->frameMax()));
    broker().events().traceDeliver(conn_.vhostName(), q->name(), d->msg, user(), conn_.name(), id_, d->redelivered);
}

std::vector<std::pair<uint64_t, Channel::Unacked>> Channel::takeUnacked(uint64_t tag, bool multiple) {
    std::vector<std::pair<uint64_t, Unacked>> taken;
    std::lock_guard<std::mutex> lock(mu_);
    if (multiple) {
        auto end = tag == 0 ? unacked_.end() : unacked_.upper_bound(tag);
        for (auto it = unacked_.begin(); it != end; ++it) taken.emplace_back(it->first, it->second);
        unacked_.erase(unacked_.begin(), end);
    } else {
        auto it = unacked_.find(tag);
        if (it != unacked_.end()) {
            taken.emplace_back(it->first, it->second);
            unacked_.erase(it);
        }
    }
    for (const auto& [t, u] : taken) {
        if (u.consumerTag.empty()) continue;
        auto c = consumers_.find(u.consumerTag);
        if (c != consumers_.end() && c->second->unacked().load() > 0) c->second->unacked()--;
    }
    return taken;
}

void Channel::settleTaken(std::vector<std::pair<uint64_t, Unacked>>& taken, Settlement how) {
    std::map<QueueBase*, std::vector<uint64_t>> byQueue;
    for (const auto& [tag, u] : taken) byQueue[u.queue].push_back(u.queueTag);
    for (auto& [queue, tags] : byQueue) queue->settle(tags, how);
}

void Channel::onBasicAck(const BasicAck& m) {
    auto taken = takeUnacked(m.deliveryTag, m.multiple);
    BrokerMetrics::get().messagesAcknowledged.inc(taken.size());
    settleTaken(taken, Settlement::Ack);
}

void Channel::onBasicNack(uint64_t tag, bool multiple, bool requeue) {
    auto taken = takeUnacked(tag, multiple);
    settleTaken(taken, requeue ? Settlement::Requeue : Settlement::Reject);
}

void Channel::onBasicRecover(bool requeue) {
    (void)requeue;  // AMQP 0-9-1 recover without requeue is deprecated; always requeue
    std::lock_guard<std::mutex> lock(mu_);
    for (auto& [tag, u] : unacked_) {
        u.queue->settle({u.queueTag}, Settlement::Requeue);
    }
    unacked_.clear();
    for (auto& [tag, c] : consumers_) c->unacked().store(0);
}

void Channel::onConfirmSelect(const ConfirmSelect& m) {
    if (txMode_) fail(kPreconditionFailed, "PRECONDITION_FAILED - cannot switch from tx to confirm mode", Method(m));
    confirmMode_ = true;
    if (!m.noWait) send(ConfirmSelectOk{});
}

void Channel::onTxSelect() {
    if (confirmMode_)
        throw AmqpException(kPreconditionFailed, "PRECONDITION_FAILED - cannot switch from confirm to tx mode", 90, 10);
    txMode_ = true;
    send(TxSelectOk{});
}

void Channel::onTxCommit() {
    if (!txMode_) throw AmqpException(kPreconditionFailed, "PRECONDITION_FAILED - channel is not transactional", 90, 20);
    auto pending = std::move(txPublishes_);
    txPublishes_.clear();
    for (auto& p : pending) publishNow(p);
    send(TxCommitOk{});
}

void Channel::onTxRollback() {
    if (!txMode_) throw AmqpException(kPreconditionFailed, "PRECONDITION_FAILED - channel is not transactional", 90, 30);
    txPublishes_.clear();
    send(TxRollbackOk{});
}

// ------------------------------------------------------------------ publishing

void Channel::handleHeader(const std::string& payload) {
    if (!pendingPublish_ || headerReceived_)
        throw AmqpException(kUnexpectedFrame, "UNEXPECTED_FRAME - content header without basic.publish", 60, 40);
    ContentHeader h = ContentHeader::decode(payload);
    if (h.classId != kClassBasic)
        throw AmqpException(kUnexpectedFrame, "UNEXPECTED_FRAME - content header for class " + std::to_string(h.classId),
                            60, 40);
    pendingProps_ = std::move(h.properties);
    pendingBodySize_ = h.bodySize;
    headerReceived_ = true;
    pendingBody_.reserve(static_cast<size_t>(h.bodySize));
    if (static_cast<int64_t>(h.bodySize) > broker().config().maxMessageSize) {
        pendingPublish_.reset();
        throw AmqpException(kPreconditionFailed,
                            "PRECONDITION_FAILED - message size " + std::to_string(h.bodySize) +
                                " is larger than configured max size " +
                                std::to_string(broker().config().maxMessageSize),
                            60, 40);
    }
    if (h.bodySize == 0) completePublish();
}

void Channel::handleBody(const std::string& payload) {
    if (!pendingPublish_ || !headerReceived_)
        throw AmqpException(kUnexpectedFrame, "UNEXPECTED_FRAME - content body without content header", 60, 40);
    pendingBody_.append(payload);
    if (pendingBody_.size() >= pendingBodySize_) completePublish();
}

void Channel::completePublish() {
    PendingPublish p;
    p.method = *pendingPublish_;
    p.props = pendingProps_ ? std::move(*pendingProps_) : BasicProperties{};
    p.body = std::move(pendingBody_);
    pendingPublish_.reset();
    pendingProps_.reset();
    pendingBody_.clear();
    pendingBodySize_ = 0;
    headerReceived_ = false;

    if (p.props.userId && *p.props.userId != user()) {
        auto u = broker().users().getUser(user());
        if (!u || !u->hasTag("impersonator")) {
            fail(kPreconditionFailed, "PRECONDITION_FAILED - user_id property set to '" + *p.props.userId +
                                          "' but authenticated user was '" + user() + "'",
                 Method(p.method));
        }
    }
    if (p.props.replyTo && *p.props.replyTo == kDirectReplyTo) {
        if (replyToAddress_.empty())
            fail(kPreconditionFailed, "PRECONDITION_FAILED - fast reply consumer does not exist", Method(p.method));
        p.props.replyTo = replyToAddress_;
    }
    if (txMode_) {
        txPublishes_.push_back(std::move(p));
        return;
    }
    publishNow(p);
}

void Channel::publishNow(PendingPublish& p) {
    publishedCount_++;
    BrokerMetrics::get().messagesPublished.inc();
    const std::string& exchange = p.method.exchange;
    const std::string& routingKey = p.method.routingKey;
    auto def = vhost().exchangeDef(exchange);
    if (def && def->type == ExchangeType::Topic &&
        !broker().users().checkTopic(user(), conn_.vhostName(), exchange, routingKey, true)) {
        fail(kAccessRefused, "ACCESS_REFUSED - write access to topic '" + routingKey + "' in exchange '" + exchange +
                                 "' in vhost '" + conn_.vhostName() + "' refused for user '" + user() + "'",
             Method(p.method));
    }

    auto msg = std::make_shared<Message>();
    msg->id = nextMessageId();
    msg->exchange = exchange;
    msg->routingKey = routingKey;
    msg->props = std::move(p.props);
    msg->body = std::move(p.body);
    msg->publishedAtMs = wallClockMs();
    msg->publisherVhost = conn_.vhostName();
    msg->publisherConnection = conn_.id();
    applyInterceptors(*msg);

    if (exchange.empty() && str::startsWith(routingKey, std::string(kDirectReplyTo) + ".")) {
        auto target = DirectReplyRegistry::instance().find(routingKey);
        if (target) target->deliverReply(msg);
        if (confirmMode_) sendConfirm(nextPublishSeq_++, true);
        return;
    }

    RouteResult r = vhost().route(exchange, msg);
    bool routed = !r.queues.empty() || r.delayed;
    if (!routed) {
        BrokerMetrics::get().messagesUnroutable.inc();
        if (p.method.mandatory) {
            BrokerMetrics::get().messagesReturned.inc();
            auto conn = connRef_.lock();
            if (conn) {
                BasicReturn ret;
                ret.replyCode = kNoRoute;
                ret.replyText = "NO_ROUTE";
                ret.exchange = exchange;
                ret.routingKey = routingKey;
                conn->sendRaw(encodeContentFrames(id_, ret, msg->props, msg->body, conn->frameMax()));
            }
        }
        if (confirmMode_) sendConfirm(nextPublishSeq_, true);
        return;
    }
    uint64_t seq = confirmMode_ ? nextPublishSeq_++ : 0;
    bool rejected = false;
    for (const auto& q : r.queues) {
        if (q->enqueue(msg) == QueueBase::EnqueueResult::Rejected) rejected = true;
    }
    if (confirmMode_) sendConfirm(seq, !rejected);
    if (vhost().tracing()) {
        std::vector<std::string> names;
        names.reserve(r.queues.size());
        for (const auto& q : r.queues) names.push_back(q->name());
        broker().events().tracePublish(conn_.vhostName(), msg, user(), conn_.name(), id_, names);
    }
}

void Channel::applyInterceptors(Message& msg) {
    const auto& cfg = broker().config();
    if (cfg.interceptTimestamp) {
        bool present = msg.props.headers && msg.props.headers->has("timestamp_in_ms");
        if (*cfg.interceptTimestamp || !present) {
            msg.props.mutableHeaders().set("timestamp_in_ms", FieldValue::int64(msg.publishedAtMs));
        }
        if (*cfg.interceptTimestamp || !msg.props.timestamp) {
            msg.props.timestamp = static_cast<uint64_t>(msg.publishedAtMs / 1000);
        }
    }
    if (cfg.interceptRoutingNode) {
        bool present = msg.props.headers && msg.props.headers->has("x-routed-by");
        if (*cfg.interceptRoutingNode || !present) {
            msg.props.mutableHeaders().set("x-routed-by", FieldValue::longString(broker().nodeName()));
        }
    }
}

void Channel::sendConfirm(uint64_t seq, bool ack) {
    BrokerMetrics::get().messagesConfirmed.inc();
    if (ack) {
        send(BasicAck{seq, false});
    } else {
        BasicNack n;
        n.deliveryTag = seq;
        n.multiple = false;
        n.requeue = false;
        send(n);
    }
}

// ------------------------------------------------------------------ delivering

bool Channel::consumerHasCredit(const ChannelConsumer& c) const {
    std::lock_guard<std::mutex> lock(mu_);
    if (!flowActive_) return false;
    if (globalPrefetch_ != 0 && unacked_.size() >= globalPrefetch_) return false;
    return c.prefetch() == 0 || const_cast<ChannelConsumer&>(c).unacked().load() <= c.prefetch();
}

void Channel::deliver(ChannelConsumer& c, QueueBase& q, Delivery d) {
    auto conn = connRef_.lock();
    if (!conn) return;
    BasicDeliver m;
    BasicProperties props = d.msg->props;
    std::string frames;
    {
        std::lock_guard<std::mutex> lock(mu_);
        m.deliveryTag = nextDeliveryTag_++;
        if (!c.noAck()) {
            Unacked u;
            u.queue = &q;
            u.queueTag = d.queueTag;
            u.consumerTag = c.tag();
            u.deliveredAtMs = wallClockMs();
            unacked_[m.deliveryTag] = u;
            c.unacked()++;
        }
        deliveredCount_++;
    }
    m.consumerTag = c.tag();
    m.redelivered = d.redelivered;
    m.exchange = d.msg->exchange;
    m.routingKey = d.msg->routingKey;
    if (q.type() == QueueType::Quorum && d.deliveryCount > 1) {
        props.mutableHeaders().set("x-delivery-count", FieldValue::int64(d.deliveryCount - 1));
    } else if (q.type() == QueueType::Stream) {
        props.mutableHeaders().set("x-stream-offset", FieldValue::int64(static_cast<int64_t>(d.streamOffset)));
    }
    frames = encodeContentFrames(id_, m, props, d.msg->body, conn->frameMax());
    conn->sendRaw(std::move(frames));
    BrokerMetrics::get().messagesDelivered.inc();
    if (d.redelivered) BrokerMetrics::get().messagesRedelivered.inc();
    broker().events().traceDeliver(conn_.vhostName(), q.name(), d.msg, user(), conn_.name(), id_, d.redelivered);
}

void Channel::deliverReply(const MessagePtr& msg) {
    auto conn = connRef_.lock();
    if (!conn || replyConsumerTag_.empty()) return;
    BasicDeliver m;
    {
        std::lock_guard<std::mutex> lock(mu_);
        m.deliveryTag = nextDeliveryTag_++;
    }
    m.consumerTag = replyConsumerTag_;
    m.exchange = msg->exchange;
    m.routingKey = msg->routingKey;
    conn->sendRaw(encodeContentFrames(id_, m, msg->props, msg->body, conn->frameMax()));
}

void Channel::consumerCancelled(const std::string& tag, const std::string& reason) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        consumers_.erase(tag);
    }
    auto conn = connRef_.lock();
    if (!conn || closing_) return;
    LOG_DEBUG("channel") << "consumer " << tag << " cancelled by server: " << reason;
    if (conn->clientCapability("consumer_cancel_notify")) conn->sendMethod(id_, BasicCancel{tag, true});
}

void Channel::checkConsumerTimeout(int64_t nowMs, int64_t timeoutMs) {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& [tag, u] : unacked_) {
        if (u.consumerTag.empty()) continue;
        if (nowMs - u.deliveredAtMs > timeoutMs) {
            throw AmqpException(kPreconditionFailed,
                                "PRECONDITION_FAILED - delivery acknowledgement on channel " + std::to_string(id_) +
                                    " timed out. Timeout value used: " + std::to_string(timeoutMs) + " ms",
                                0, 0);
        }
    }
}

void Channel::shutdown() {
    if (shutdownDone_) return;
    shutdownDone_ = true;
    closing_.store(true);
    std::map<std::string, std::shared_ptr<ChannelConsumer>> consumers;
    std::map<uint64_t, Unacked> unacked;
    {
        std::lock_guard<std::mutex> lock(mu_);
        consumers.swap(consumers_);
        unacked.swap(unacked_);
    }
    if (!replyToAddress_.empty()) {
        DirectReplyRegistry::instance().unregister(replyToAddress_);
        replyToAddress_.clear();
    }
    std::vector<QueuePtr> touched;
    for (auto& [tag, c] : consumers) {
        if (auto q = c->queue()) {
            q->removeConsumer(tag, conn_.id());
            touched.push_back(q);
        }
    }
    std::map<QueueBase*, std::vector<uint64_t>> requeue;
    for (auto& [tag, u] : unacked) {
        if (u.consumerTag.empty()) continue;
        requeue[u.queue].push_back(u.queueTag);
    }
    for (auto& [q, tags] : requeue) q->settle(tags, Settlement::Requeue);
    if (conn_.vhost()) {
        for (auto& q : touched) conn_.vhost()->maybeAutoDelete(q);
    }
}

size_t Channel::unackedCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return unacked_.size();
}

size_t Channel::consumerCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return consumers_.size();
}

Json Channel::info() const {
    std::lock_guard<std::mutex> lock(mu_);
    Json j = Json::object();
    j["number"] = id_;
    j["consumer_count"] = static_cast<int64_t>(consumers_.size());
    j["messages_unacknowledged"] = static_cast<int64_t>(unacked_.size());
    j["prefetch_count"] = prefetchCount_;
    j["global_prefetch_count"] = globalPrefetch_;
    j["confirm"] = confirmMode_;
    j["transactional"] = txMode_;
    j["publish"] = static_cast<int64_t>(publishedCount_);
    j["deliver"] = static_cast<int64_t>(deliveredCount_);
    return j;
}

}  // namespace tq::broker
