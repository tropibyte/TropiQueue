// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "amqp/field_table.h"
#include "broker/message.h"
#include "common/json.h"

namespace tq::broker {

class QueueBase;

enum class QueueType { Classic, Quorum, Stream };

const char* queueTypeName(QueueType t);
std::optional<QueueType> parseQueueType(const std::string& name);

struct QueueDef {
    std::string name;
    std::string vhost;
    QueueType type = QueueType::Classic;
    bool durable = true;
    bool exclusive = false;
    bool autoDelete = false;
    amqp::FieldTable arguments;
    uint64_t exclusiveOwner = 0;  // connection id for exclusive queues
    std::string node;             // node that hosts a classic queue
    std::vector<int> replicas;    // cluster node ids holding a quorum queue replica

    Json toJson() const;
    static std::optional<QueueDef> fromJson(const Json& j);
};

// A message handed to a consumer or returned by basic.get.
struct Delivery {
    MessagePtr msg;
    uint64_t queueTag = 0;        // queue-local identifier used to settle
    bool redelivered = false;
    uint32_t deliveryCount = 0;
    uint64_t streamOffset = 0;    // streams only
    uint32_t remaining = 0;       // messages left (basic.get-ok message-count)
};

enum class Settlement { Ack, Requeue, Reject };

// Implemented by channels for each basic.consume.
class Consumer {
public:
    virtual ~Consumer() = default;
    virtual const std::string& tag() const = 0;
    virtual bool noAck() const = 0;
    virtual int priority() const = 0;
    virtual uint64_t connectionId() const = 0;
    virtual bool exclusive() const = 0;
    virtual const amqp::FieldTable& arguments() const = 0;
    virtual uint16_t prefetchCount() const { return 0; }
    virtual bool hasCredit() const = 0;
    virtual void deliver(QueueBase& queue, Delivery delivery) = 0;
    virtual void cancelledByServer(const std::string& reason) = 0;
};

using ConsumerPtr = std::shared_ptr<Consumer>;

// Implemented by the virtual host; receives messages that a queue expires,
// rejects or drops so they can be republished to the dead-letter exchange.
class DeadLetterSink {
public:
    virtual ~DeadLetterSink() = default;
    virtual void deadLetter(const QueueBase& from, const MessagePtr& msg, const std::string& reason) = 0;
};

// Durable storage for one queue's messages (see storage/queue_store.h).
class QueueStorage {
public:
    struct Recovered {
        uint64_t tag;
        MessagePtr msg;
        bool delivered;
    };
    virtual ~QueueStorage() = default;
    virtual void recordPublish(uint64_t tag, const MessagePtr& msg) = 0;
    virtual void recordDelivered(uint64_t tag) = 0;
    virtual void recordAck(uint64_t tag) = 0;
    virtual std::vector<Recovered> recover() = 0;
    virtual void flush() = 0;
    virtual void destroy() = 0;
};

struct QueueCounters {
    std::atomic<uint64_t> published{0};
    std::atomic<uint64_t> delivered{0};
    std::atomic<uint64_t> acked{0};
    std::atomic<uint64_t> redelivered{0};
    std::atomic<uint64_t> expired{0};
    std::atomic<uint64_t> deadLettered{0};
    std::atomic<uint64_t> dropped{0};
};

class QueueBase : public std::enable_shared_from_this<QueueBase> {
public:
    enum class EnqueueResult { Accepted, Rejected };

    explicit QueueBase(QueueDef def);
    virtual ~QueueBase() = default;

    const QueueDef& def() const { return def_; }
    const std::string& name() const { return def_.name; }
    const std::string& vhost() const { return def_.vhost; }
    QueueType type() const { return def_.type; }
    bool durable() const { return def_.durable; }
    bool exclusive() const { return def_.exclusive; }
    bool autoDelete() const { return def_.autoDelete; }
    uint64_t exclusiveOwner() const { return def_.exclusiveOwner; }

    // Arguments after policies are applied; set before the queue is used.
    void setEffectiveArguments(amqp::FieldTable args);
    const amqp::FieldTable& effectiveArguments() const { return effectiveArgs_; }
    void setDeadLetterSink(DeadLetterSink* sink) { deadLetterSink_ = sink; }

    virtual EnqueueResult enqueue(const MessagePtr& msg) = 0;
    virtual std::optional<Delivery> basicGet(bool noAck) = 0;
    virtual bool addConsumer(const ConsumerPtr& consumer, std::string* error) = 0;
    virtual void removeConsumer(const std::string& tag, uint64_t connectionId) = 0;
    virtual void settle(const std::vector<uint64_t>& tags, Settlement how) = 0;
    virtual void creditAvailable() = 0;
    virtual uint32_t purge() = 0;
    virtual void tick(int64_t nowMs) = 0;
    virtual void deleted() = 0;
    virtual void recover() {}

    virtual size_t messageCount() const = 0;
    virtual size_t unackedCount() const = 0;
    virtual size_t consumerCount() const = 0;
    virtual uint64_t messageBytes() const = 0;

    // True when x-expires has elapsed and the queue should be deleted.
    virtual bool expired(int64_t nowMs) const;
    void touch(int64_t nowMs) { lastUsedMs_.store(nowMs); }

    virtual Json info() const;
    QueueCounters& counters() { return counters_; }

protected:
    QueueDef def_;
    amqp::FieldTable effectiveArgs_;
    DeadLetterSink* deadLetterSink_ = nullptr;
    QueueCounters counters_;
    std::atomic<int64_t> lastUsedMs_{0};
    int64_t expiresMs_ = 0;
};

using QueuePtr = std::shared_ptr<QueueBase>;

}  // namespace tq::broker
