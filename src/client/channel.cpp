// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "client/channel.h"

#include "amqp/constants.h"
#include "client/connection.h"
#include "common/clock.h"
#include "common/logging.h"

namespace tq::client {

using namespace tq::amqp;

namespace {
constexpr int kRpcTimeoutMs = 15000;
}

Channel::Channel(std::weak_ptr<Connection> conn, uint16_t id) : conn_(std::move(conn)), id_(id) {}

bool Channel::isOpen() const {
    std::lock_guard<std::mutex> lock(mu_);
    return open_;
}

std::string Channel::closeReason() const {
    std::lock_guard<std::mutex> lock(mu_);
    return closeReason_;
}

uint16_t Channel::closeCode() const {
    std::lock_guard<std::mutex> lock(mu_);
    return closeCode_;
}

void Channel::throwIfClosed() const {
    std::lock_guard<std::mutex> lock(mu_);
    if (!open_) throw ClientError(closeCode_ ? closeCode_ : static_cast<uint16_t>(kChannelError), closeReason_.empty() ? "channel closed" : closeReason_);
}

void Channel::send(const Method& m) {
    auto conn = conn_.lock();
    if (!conn) throw ClientError(kChannelError, "connection is gone");
    conn->sendMethod(id_, m);
}

Method Channel::rpc(const Method& request, uint16_t expectedClass, uint16_t expectedMethod) {
    throwIfClosed();
    send(request);
    std::unique_lock<std::mutex> lock(mu_);
    bool ok = cv_.wait_for(lock, std::chrono::milliseconds(kRpcTimeoutMs), [&] { return !replies_.empty() || !open_; });
    if (!open_) throw ClientError(closeCode_, closeReason_);
    if (!ok) throw ClientError(kInternalError, "timeout waiting for reply to " + methodName(request));
    Method reply = std::move(replies_.front());
    replies_.pop_front();
    if (classIdOf(reply) != expectedClass || methodIdOf(reply) != expectedMethod) {
        // basic.get may legitimately answer with get-empty.
        if (!(expectedClass == 60 && expectedMethod == 71 && methodAs<BasicGetEmpty>(reply))) {
            throw ClientError(kUnexpectedFrame, "unexpected reply " + methodName(reply) + " to " + methodName(request));
        }
    }
    return reply;
}

// ----------------------------------------------------------------- topology

void Channel::declareExchange(const std::string& name, const std::string& type, bool durable, bool autoDelete,
                              bool internal, const FieldTable& args) {
    ExchangeDeclare m;
    m.exchange = name;
    m.type = type;
    m.durable = durable;
    m.autoDelete = autoDelete;
    m.internal = internal;
    m.arguments = args;
    rpc(m, 40, 11);
}

void Channel::deleteExchange(const std::string& name, bool ifUnused) {
    ExchangeDelete m;
    m.exchange = name;
    m.ifUnused = ifUnused;
    rpc(m, 40, 21);
}

void Channel::bindExchange(const std::string& destination, const std::string& source, const std::string& routingKey,
                           const FieldTable& args) {
    ExchangeBind m;
    m.destination = destination;
    m.source = source;
    m.routingKey = routingKey;
    m.arguments = args;
    rpc(m, 40, 31);
}

QueueInfo Channel::declareQueue(const std::string& name, bool durable, bool exclusive, bool autoDelete,
                                const FieldTable& args) {
    QueueDeclare m;
    m.queue = name;
    m.durable = durable;
    m.exclusive = exclusive;
    m.autoDelete = autoDelete;
    m.arguments = args;
    Method r = rpc(m, 50, 11);
    auto* ok = methodAs<QueueDeclareOk>(r);
    return QueueInfo{ok->queue, ok->messageCount, ok->consumerCount};
}

QueueInfo Channel::declareQueuePassive(const std::string& name) {
    QueueDeclare m;
    m.queue = name;
    m.passive = true;
    Method r = rpc(m, 50, 11);
    auto* ok = methodAs<QueueDeclareOk>(r);
    return QueueInfo{ok->queue, ok->messageCount, ok->consumerCount};
}

void Channel::bindQueue(const std::string& queue, const std::string& exchange, const std::string& routingKey,
                        const FieldTable& args) {
    QueueBind m;
    m.queue = queue;
    m.exchange = exchange;
    m.routingKey = routingKey;
    m.arguments = args;
    rpc(m, 50, 21);
}

void Channel::unbindQueue(const std::string& queue, const std::string& exchange, const std::string& routingKey,
                          const FieldTable& args) {
    QueueUnbind m;
    m.queue = queue;
    m.exchange = exchange;
    m.routingKey = routingKey;
    m.arguments = args;
    rpc(m, 50, 51);
}

uint32_t Channel::purgeQueue(const std::string& name) {
    QueuePurge m;
    m.queue = name;
    Method r = rpc(m, 50, 31);
    return methodAs<QueuePurgeOk>(r)->messageCount;
}

uint32_t Channel::deleteQueue(const std::string& name, bool ifUnused, bool ifEmpty) {
    QueueDelete m;
    m.queue = name;
    m.ifUnused = ifUnused;
    m.ifEmpty = ifEmpty;
    Method r = rpc(m, 50, 41);
    return methodAs<QueueDeleteOk>(r)->messageCount;
}

// ---------------------------------------------------------------- messaging

void Channel::qos(uint16_t prefetchCount, bool global) {
    BasicQos m;
    m.prefetchCount = prefetchCount;
    m.global = global;
    rpc(m, 60, 11);
}

void Channel::publish(const std::string& exchange, const std::string& routingKey, const std::string& body,
                      const BasicProperties& props, bool mandatory) {
    auto conn = conn_.lock();
    if (!conn) throw ClientError(kChannelError, "connection is gone");
    BasicPublish m;
    m.exchange = exchange;
    m.routingKey = routingKey;
    m.mandatory = mandatory;
    if (confirmMode_) {
        std::lock_guard<std::mutex> lock(mu_);
        pendingConfirms_[nextPublishSeq_++] = false;
    }
    conn->sendFrames(encodeContentFrames(id_, m, props, body, conn->frameMax()));
}

std::string Channel::consume(const std::string& queue, ConsumerCallback cb, bool noAck, const std::string& tag,
                             bool exclusive, const FieldTable& args) {
    BasicConsume m;
    m.queue = queue;
    m.consumerTag = tag;
    m.noAck = noAck;
    m.exclusive = exclusive;
    m.arguments = args;
    {
        std::lock_guard<std::mutex> lock(mu_);
        consumers_[tag.empty() ? "__pending__" : tag] = cb;
    }
    Method r;
    try {
        r = rpc(m, 60, 21);
    } catch (...) {
        std::lock_guard<std::mutex> lock(mu_);
        consumers_.erase(tag.empty() ? "__pending__" : tag);
        throw;
    }
    std::string actual = methodAs<BasicConsumeOk>(r)->consumerTag;
    if (tag.empty()) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = consumers_.find("__pending__");
        if (it != consumers_.end()) {
            consumers_[actual] = it->second;
            consumers_.erase(it);
        }
    }
    return actual;
}

void Channel::cancel(const std::string& consumerTag) {
    BasicCancel m;
    m.consumerTag = consumerTag;
    rpc(m, 60, 31);
    std::lock_guard<std::mutex> lock(mu_);
    consumers_.erase(consumerTag);
}

std::optional<Delivery> Channel::get(const std::string& queue, bool noAck) {
    BasicGet m;
    m.queue = queue;
    m.noAck = noAck;
    Method r = rpc(m, 60, 71);
    if (methodAs<BasicGetEmpty>(r)) return std::nullopt;
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait_for(lock, std::chrono::milliseconds(kRpcTimeoutMs), [&] { return !gets_.empty() || !open_; });
    if (gets_.empty()) throw ClientError(kInternalError, "timeout waiting for basic.get content");
    Delivery d = std::move(gets_.front());
    gets_.pop_front();
    return d;
}

void Channel::ack(uint64_t deliveryTag, bool multiple) { send(BasicAck{deliveryTag, multiple}); }

void Channel::nack(uint64_t deliveryTag, bool multiple, bool requeue) {
    BasicNack m;
    m.deliveryTag = deliveryTag;
    m.multiple = multiple;
    m.requeue = requeue;
    send(m);
}

void Channel::reject(uint64_t deliveryTag, bool requeue) { send(BasicReject{deliveryTag, requeue}); }

void Channel::recover(bool requeue) { rpc(BasicRecover{requeue}, 60, 111); }

void Channel::onReturn(ReturnCallback cb) {
    std::lock_guard<std::mutex> lock(mu_);
    returnCallback_ = std::move(cb);
}

// --------------------------------------------------------- confirms and tx

void Channel::confirmSelect() {
    rpc(ConfirmSelect{}, 85, 11);
    std::lock_guard<std::mutex> lock(mu_);
    confirmMode_ = true;
}

bool Channel::waitForConfirms(int timeoutMs) {
    std::unique_lock<std::mutex> lock(mu_);
    bool done = cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                             [&] { return pendingConfirms_.empty() || !open_; });
    bool ok = done && open_ && !anyNacked_;
    anyNacked_ = false;
    return ok;
}

uint64_t Channel::unconfirmedCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return pendingConfirms_.size();
}

void Channel::txSelect() { rpc(TxSelect{}, 90, 11); }
void Channel::txCommit() { rpc(TxCommit{}, 90, 21); }
void Channel::txRollback() { rpc(TxRollback{}, 90, 31); }

void Channel::close() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!open_) return;
    }
    ChannelClose m;
    m.replyCode = kReplySuccess;
    m.replyText = "Goodbye";
    try {
        rpc(m, 20, 41);
    } catch (const ClientError&) {
    }
    std::lock_guard<std::mutex> lock(mu_);
    open_ = false;
}

// ------------------------------------------------------------ reader thread

void Channel::connectionClosed(uint16_t code, const std::string& text) {
    std::lock_guard<std::mutex> lock(mu_);
    if (open_) {
        open_ = false;
        closeCode_ = code;
        closeReason_ = text;
    }
    cv_.notify_all();
}

void Channel::deliverAssembled() {
    Delivery d;
    d.properties = contentProps_ ? std::move(*contentProps_) : BasicProperties{};
    d.body = std::move(contentBody_);
    Method m = std::move(*contentMethod_);
    contentMethod_.reset();
    contentProps_.reset();
    contentBody_.clear();
    contentSize_ = 0;
    if (auto* del = methodAs<BasicDeliver>(m)) {
        d.consumerTag = del->consumerTag;
        d.deliveryTag = del->deliveryTag;
        d.redelivered = del->redelivered;
        d.exchange = del->exchange;
        d.routingKey = del->routingKey;
        auto it = consumers_.find(d.consumerTag);
        if (it == consumers_.end()) it = consumers_.find("__pending__");
        if (it != consumers_.end() && it->second) it->second(d);
    } else if (auto* ret = methodAs<BasicReturn>(m)) {
        ReturnedMessage r;
        r.replyCode = ret->replyCode;
        r.replyText = ret->replyText;
        r.exchange = ret->exchange;
        r.routingKey = ret->routingKey;
        r.properties = std::move(d.properties);
        r.body = std::move(d.body);
        if (returnCallback_) returnCallback_(r);
    } else if (auto* get = methodAs<BasicGetOk>(m)) {
        d.deliveryTag = get->deliveryTag;
        d.redelivered = get->redelivered;
        d.exchange = get->exchange;
        d.routingKey = get->routingKey;
        d.messageCount = get->messageCount;
        gets_.push_back(std::move(d));
        cv_.notify_all();
    }
}

void Channel::handleFrame(uint8_t type, const std::string& payload) {
    std::unique_lock<std::mutex> lock(mu_);
    if (type == 0) return;  // channel created locally
    if (type == kFrameHeader) {
        if (!contentMethod_) return;
        ContentHeader h = ContentHeader::decode(payload);
        contentProps_ = std::move(h.properties);
        contentSize_ = h.bodySize;
        if (contentSize_ == 0) deliverAssembled();
        return;
    }
    if (type == kFrameBody) {
        if (!contentMethod_) return;
        contentBody_.append(payload);
        if (contentBody_.size() >= contentSize_) deliverAssembled();
        return;
    }
    Method m = decodeMethod(payload);
    if (carriesContent(m)) {
        if (methodAs<BasicGetOk>(m)) replies_.push_back(m);
        contentMethod_ = std::move(m);
        cv_.notify_all();
        return;
    }
    if (auto* close = methodAs<ChannelClose>(m)) {
        open_ = false;
        closeCode_ = close->replyCode;
        closeReason_ = close->replyText;
        cv_.notify_all();
        lock.unlock();
        if (auto conn = conn_.lock()) conn->sendMethod(id_, ChannelCloseOk{});
        return;
    }
    if (methodAs<ChannelOpenOk>(m)) return;
    if (auto* ack = methodAs<BasicAck>(m); ack && confirmMode_) {
        if (ack->multiple) {
            for (auto it = pendingConfirms_.begin(); it != pendingConfirms_.end(); ++it) {
                if (it->first > ack->deliveryTag) break;
                pendingConfirms_.erase(it);
            }
        } else {
            pendingConfirms_.erase(ack->deliveryTag);
        }
        cv_.notify_all();
        return;
    }
    if (auto* nack = methodAs<BasicNack>(m); nack && confirmMode_) {
        anyNacked_ = true;
        if (nack->multiple) {
            auto end = pendingConfirms_.upper_bound(nack->deliveryTag);
            pendingConfirms_.erase(pendingConfirms_.begin(), end);
        } else {
            pendingConfirms_.erase(nack->deliveryTag);
        }
        cv_.notify_all();
        return;
    }
    if (auto* cancel = methodAs<BasicCancel>(m)) {
        consumers_.erase(cancel->consumerTag);
        return;
    }
    replies_.push_back(std::move(m));
    cv_.notify_all();
}

}  // namespace tq::client
