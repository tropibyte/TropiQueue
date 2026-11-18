// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "broker/queue.h"

namespace tq::broker {

enum class OverflowMode { DropHead, RejectPublish, RejectPublishDlx };

// In-memory FIFO queue with optional persistence of persistent messages,
// priorities, TTLs, length limits, dead-lettering and single active consumer.
class ClassicQueue : public QueueBase {
public:
    ClassicQueue(QueueDef def, std::unique_ptr<QueueStorage> storage = nullptr);
    ~ClassicQueue() override;

    EnqueueResult enqueue(const MessagePtr& msg) override;
    std::optional<Delivery> basicGet(bool noAck) override;
    bool addConsumer(const ConsumerPtr& consumer, std::string* error) override;
    void removeConsumer(const std::string& tag, uint64_t connectionId) override;
    void settle(const std::vector<uint64_t>& tags, Settlement how) override;
    void creditAvailable() override;
    uint32_t purge() override;
    void tick(int64_t nowMs) override;
    void deleted() override;
    void recover() override;

    size_t messageCount() const override;
    size_t unackedCount() const override;
    size_t consumerCount() const override;
    uint64_t messageBytes() const override;
    Json info() const override;

    // Re-reads limits and TTLs after a policy change.
    void reconfigure();

private:
    struct Entry {
        MessagePtr msg;
        uint64_t tag = 0;
        int64_t expiresAt = 0;  // wall clock ms, 0 = never
        bool redelivered = false;
        uint32_t deliveryCount = 0;
    };

    size_t levelFor(const Message& msg) const;
    size_t readyCountLocked() const;
    bool overLimitLocked() const;
    void dropHeadLocked();
    std::optional<Entry> popNextLocked(int64_t nowMs);
    ConsumerPtr pickConsumerLocked();
    void tryDeliverLocked();
    void releaseEntryLocked(const Entry& e);
    void deadLetterLocked(const Entry& e, const std::string& reason);
    // Hands collected dead letters to the sink; must be called without mu_ held.
    void flushDeadLetters();
    int64_t expiryFor(const Message& msg, int64_t nowMs) const;

    mutable std::mutex mu_;
    std::vector<std::deque<Entry>> levels_;
    std::map<uint64_t, Entry> unacked_;
    std::vector<ConsumerPtr> consumers_;
    ConsumerPtr activeConsumer_;  // single active consumer
    size_t rrIndex_ = 0;
    uint64_t nextTag_ = 1;
    uint64_t bytes_ = 0;
    std::unique_ptr<QueueStorage> storage_;
    std::vector<std::pair<MessagePtr, std::string>> pendingDeadLetters_;
    bool deleted_ = false;
    bool delivering_ = false;

    // Configuration (from effective arguments).
    int64_t messageTtlMs_ = -1;
    int64_t maxLength_ = -1;
    int64_t maxLengthBytes_ = -1;
    OverflowMode overflow_ = OverflowMode::DropHead;
    int maxPriority_ = 0;
    bool singleActiveConsumer_ = false;
};

}  // namespace tq::broker
