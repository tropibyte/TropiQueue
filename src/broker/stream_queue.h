// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "broker/queue.h"
#include "storage/stream_log.h"

namespace tq::broker {

// A stream exposed as an AMQP queue: messages are never removed by consumers;
// each consumer reads from its own offset (x-stream-offset) and acks only to
// release credit.
class StreamQueue : public QueueBase {
public:
    StreamQueue(QueueDef def, storage::StreamManager& manager);
    ~StreamQueue() override;

    bool init(std::string* error);

    EnqueueResult enqueue(const MessagePtr& msg) override;
    std::optional<Delivery> basicGet(bool noAck) override;
    bool addConsumer(const ConsumerPtr& consumer, std::string* error) override;
    void removeConsumer(const std::string& tag, uint64_t connectionId) override;
    void settle(const std::vector<uint64_t>& tags, Settlement how) override;
    void creditAvailable() override;
    uint32_t purge() override { return 0; }
    void tick(int64_t nowMs) override;
    void deleted() override;

    size_t messageCount() const override;
    size_t unackedCount() const override;
    size_t consumerCount() const override;
    uint64_t messageBytes() const override;
    Json info() const override;

    static int64_t parseMaxAge(const std::string& spec);  // "7D", "12h", "30m", "10s", "1Y"

private:
    struct Reader {
        ConsumerPtr consumer;
        uint64_t nextOffset = 0;
        uint32_t inflight = 0;
        std::string name;  // x-stream-consumer-name for offset tracking
        uint64_t lastAcked = 0;
        bool acked = false;
    };

    uint64_t startOffset(const amqp::FieldTable& args, const std::string& name) const;
    void pumpLocked();

    storage::StreamManager& manager_;
    std::shared_ptr<storage::StreamLog> log_;
    mutable std::mutex mu_;
    std::map<std::string, Reader> readers_;  // key: connection id + tag
    std::map<uint64_t, std::pair<std::string, uint64_t>> inflight_;  // queue tag -> (reader key, offset)
    uint64_t nextTag_ = 1;
    bool pumping_ = false;
};

}  // namespace tq::broker
