// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "amqp/methods.h"
#include "amqp/properties.h"
#include "broker/queue.h"
#include "common/json.h"

namespace tq::broker {

class AmqpConnection;
class Broker;
class Channel;
class VirtualHost;

// Raised while processing a method; converted to channel.close (soft errors)
// or connection.close (hard errors) by the connection.
class AmqpException : public std::runtime_error {
public:
    AmqpException(uint16_t code, const std::string& text, uint16_t classId = 0, uint16_t methodId = 0)
        : std::runtime_error(text), code(code), classId(classId), methodId(methodId) {}
    uint16_t code;
    uint16_t classId;
    uint16_t methodId;
};

class ChannelConsumer : public Consumer {
public:
    ChannelConsumer(std::weak_ptr<Channel> channel, std::string tag, std::weak_ptr<QueueBase> queue, bool noAck,
                    bool exclusive, int priority, uint16_t prefetch, amqp::FieldTable args, uint64_t connectionId);

    const std::string& tag() const override { return tag_; }
    bool noAck() const override { return noAck_; }
    int priority() const override { return priority_; }
    uint64_t connectionId() const override { return connectionId_; }
    bool exclusive() const override { return exclusive_; }
    const amqp::FieldTable& arguments() const override { return args_; }
    uint16_t prefetchCount() const override { return prefetch_; }
    bool hasCredit() const override;
    void deliver(QueueBase& queue, Delivery delivery) override;
    void cancelledByServer(const std::string& reason) override;

    std::shared_ptr<QueueBase> queue() const { return queue_.lock(); }
    std::atomic<uint32_t>& unacked() { return unacked_; }
    uint16_t prefetch() const { return prefetch_; }

private:
    std::weak_ptr<Channel> channel_;
    std::string tag_;
    std::weak_ptr<QueueBase> queue_;
    bool noAck_;
    bool exclusive_;
    int priority_;
    uint16_t prefetch_;
    amqp::FieldTable args_;
    uint64_t connectionId_;
    std::atomic<uint32_t> unacked_{0};
};

// One AMQP channel. Methods arrive on the connection's I/O thread; deliveries
// may be pushed from any thread that enqueues into a queue.
class Channel : public std::enable_shared_from_this<Channel> {
public:
    Channel(AmqpConnection& conn, uint16_t id);
    ~Channel();

    uint16_t id() const { return id_; }
    bool closing() const { return closing_.load(); }
    void markClosing() { closing_.store(true); }

    void handleMethod(const amqp::Method& method);
    void handleHeader(const std::string& payload);
    void handleBody(const std::string& payload);
    bool expectingContent() const { return pendingPublish_.has_value(); }

    // Releases consumers and requeues unacknowledged deliveries.
    void shutdown();
    // Closes the channel with an error if a delivery is older than the timeout.
    void checkConsumerTimeout(int64_t nowMs, int64_t timeoutMs);

    // Called by ChannelConsumer.
    bool consumerHasCredit(const ChannelConsumer& c) const;
    void deliver(ChannelConsumer& c, QueueBase& q, Delivery d);
    void consumerCancelled(const std::string& tag, const std::string& reason);
    // Direct reply-to delivery.
    void deliverReply(const MessagePtr& msg);

    Json info() const;
    size_t unackedCount() const;
    size_t consumerCount() const;

private:
    struct Unacked {
        QueueBase* queue = nullptr;
        uint64_t queueTag = 0;
        std::string consumerTag;
        int64_t deliveredAtMs = 0;
    };
    struct PendingPublish {
        amqp::BasicPublish method;
        amqp::BasicProperties props;
        std::string body;
    };

    Broker& broker();
    VirtualHost& vhost();
    const std::string& user() const;
    [[noreturn]] void fail(uint16_t code, const std::string& text, const amqp::Method& m) const;
    void checkAccess(int access, const std::string& resource, const amqp::Method& m);
    void send(const amqp::Method& m);

    void onExchangeDeclare(const amqp::ExchangeDeclare& m);
    void onExchangeDelete(const amqp::ExchangeDelete& m);
    void onExchangeBind(const amqp::ExchangeBind& m, bool bind);
    void onExchangeUnbind(const amqp::ExchangeUnbind& m);
    void onQueueDeclare(const amqp::QueueDeclare& m);
    void onQueueBind(const amqp::QueueBind& m);
    void onQueueUnbind(const amqp::QueueUnbind& m);
    void onQueuePurge(const amqp::QueuePurge& m);
    void onQueueDelete(const amqp::QueueDelete& m);
    void onBasicQos(const amqp::BasicQos& m);
    void onBasicConsume(const amqp::BasicConsume& m);
    void onBasicCancel(const amqp::BasicCancel& m);
    void onBasicGet(const amqp::BasicGet& m);
    void onBasicAck(const amqp::BasicAck& m);
    void onBasicNack(uint64_t tag, bool multiple, bool requeue);
    void onBasicRecover(bool requeue);
    void onConfirmSelect(const amqp::ConfirmSelect& m);
    void onTxSelect();
    void onTxCommit();
    void onTxRollback();

    void completePublish();
    void publishNow(PendingPublish& p);
    void sendConfirm(uint64_t seq, bool ack);
    void applyInterceptors(Message& msg);
    std::vector<std::pair<uint64_t, Unacked>> takeUnacked(uint64_t tag, bool multiple);
    void settleTaken(std::vector<std::pair<uint64_t, Unacked>>& taken, Settlement how);
    QueuePtr findQueueOrFail(const std::string& name, const amqp::Method& m);
    void validateQueueArguments(const amqp::FieldTable& args, const amqp::Method& m);

    AmqpConnection& conn_;
    std::weak_ptr<AmqpConnection> connRef_;
    uint16_t id_;
    std::atomic<bool> closing_{false};

    mutable std::mutex mu_;
    std::map<uint64_t, Unacked> unacked_;
    uint64_t nextDeliveryTag_ = 1;
    std::map<std::string, std::shared_ptr<ChannelConsumer>> consumers_;
    uint16_t prefetchCount_ = 0;
    uint16_t globalPrefetch_ = 0;
    bool flowActive_ = true;

    bool confirmMode_ = false;
    uint64_t nextPublishSeq_ = 1;
    bool txMode_ = false;
    std::vector<PendingPublish> txPublishes_;

    std::optional<amqp::BasicPublish> pendingPublish_;
    std::optional<amqp::BasicProperties> pendingProps_;
    uint64_t pendingBodySize_ = 0;
    std::string pendingBody_;
    bool headerReceived_ = false;
    bool shutdownDone_ = false;

    std::string replyToAddress_;  // direct reply-to pseudo queue
    std::string replyConsumerTag_;
    uint64_t publishedCount_ = 0;
    uint64_t deliveredCount_ = 0;
};

// Registry of channels consuming from amq.rabbitmq.reply-to.
class DirectReplyRegistry {
public:
    static DirectReplyRegistry& instance();
    std::string registerChannel(const std::shared_ptr<Channel>& ch);
    void unregister(const std::string& address);
    std::shared_ptr<Channel> find(const std::string& address);

private:
    std::mutex mu_;
    std::map<std::string, std::weak_ptr<Channel>> channels_;
};

constexpr const char* kDirectReplyTo = "amq.rabbitmq.reply-to";

}  // namespace tq::broker
