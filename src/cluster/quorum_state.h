// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "broker/message.h"
#include "cluster/raft.h"

namespace tq::cluster {

// Deterministic state machine behind a quorum queue. Every replica applies
// the same commands in the same order and therefore computes the same
// message-to-consumer assignments; each node only acts on the effects that
// concern its own consumers.
class QuorumStateMachine : public StateMachine {
public:
    struct Config {
        int64_t deliveryLimit = -1;   // x-delivery-limit
        int64_t maxLength = -1;       // x-max-length
        int64_t maxLengthBytes = -1;  // x-max-length-bytes
        bool rejectPublish = false;   // x-overflow=reject-publish
        int64_t messageTtlMs = -1;    // x-message-ttl
        bool deadLetter = false;      // x-dead-letter-exchange is set
    };

    struct Effect {
        enum class Kind { Deliver, DeadLetter } kind = Kind::Deliver;
        std::string consumer;
        uint64_t id = 0;
        broker::MessagePtr msg;
        uint32_t deliveryCount = 0;
        std::string reason;
    };

    // Command encoders (the proposer builds these).
    static std::string enqueueCommand(const broker::Message& msg, int64_t nowMs);
    static std::string checkoutCommand(const std::string& consumer, uint16_t prefetch, bool noAck);
    static std::string cancelCommand(const std::string& consumer);
    static std::string settleCommand(const std::vector<uint64_t>& ids, uint8_t op);  // 0 ack, 1 requeue, 2 reject
    static std::string getCommand(bool noAck, const std::string& getter);
    static std::string purgeCommand();
    static std::string tickCommand(int64_t nowMs);

    explicit QuorumStateMachine(Config config);

    std::string apply(uint64_t index, const std::string& command) override;
    std::string snapshot() override;
    void restore(const std::string& snapshot) override;

    std::vector<Effect> takeEffects();
    void reconfigure(Config config);

    size_t readyCount() const;
    size_t checkedOutCount() const;
    size_t consumerCount() const;
    uint64_t bytes() const;

private:
    struct Entry {
        broker::MessagePtr msg;
        uint32_t deliveryCount = 0;  // number of returns so far
        int64_t enqueuedAt = 0;
        std::string holder;          // consumer key while checked out
    };
    struct ConsumerState {
        uint16_t prefetch = 0;
        bool noAck = false;
        std::set<uint64_t> checkedOut;
    };

    void assignLocked();
    void returnLocked(uint64_t id);
    void dropLocked(uint64_t id, const std::string& reason);
    void expireLocked(int64_t nowMs);

    mutable std::mutex mu_;
    Config config_;
    std::map<uint64_t, Entry> messages_;
    std::deque<uint64_t> ready_;
    std::map<std::string, ConsumerState> consumers_;
    std::vector<std::string> order_;  // round-robin order of consumers
    size_t rr_ = 0;
    uint64_t bytes_ = 0;
    std::vector<Effect> effects_;
};

}  // namespace tq::cluster
