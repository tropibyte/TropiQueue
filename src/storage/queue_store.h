// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "broker/queue.h"
#include "common/fileio.h"
#include "storage/message_store.h"

namespace tq::storage {

// Per-queue journal of publish/deliver/ack records. Message bodies live in
// the shared MessageStore; the journal only records which messages the queue
// still owns.
//
// Record layout: type(1) tag(8) message-id(8)
class QueueStore : public broker::QueueStorage {
public:
    QueueStore(MessageStore& store, std::string path);
    ~QueueStore() override;

    void recordPublish(uint64_t tag, const broker::MessagePtr& msg) override;
    void recordDelivered(uint64_t tag) override;
    void recordAck(uint64_t tag) override;
    std::vector<Recovered> recover() override;
    void flush() override;
    void destroy() override;

    const std::string& path() const { return path_; }
    size_t liveEntries() const;

    static std::string pathFor(const std::string& dataDir, const std::string& vhost, const std::string& queue);

private:
    enum RecordType : uint8_t { kPublish = 1, kDeliver = 2, kAck = 3 };
    struct Live {
        uint64_t messageId = 0;
        bool delivered = false;
    };

    bool ensureOpen();
    void appendRecord(RecordType type, uint64_t tag, uint64_t messageId);
    void maybeCompactLocked();

    MessageStore& store_;
    std::string path_;
    mutable std::mutex mu_;
    fs::File file_;
    std::map<uint64_t, Live> live_;
    uint64_t ackedSinceCompaction_ = 0;
    bool dirty_ = false;
    bool destroyed_ = false;
};

}  // namespace tq::storage
