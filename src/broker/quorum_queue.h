// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "broker/queue.h"
#include "cluster/quorum_state.h"

namespace tq::cluster {
class Cluster;
class RaftNode;
}  // namespace tq::cluster

namespace tq::broker {

// Replicated queue backed by a Raft group (see cluster/quorum_state.h).
class QuorumQueue : public QueueBase {
public:
    QuorumQueue(QueueDef def, cluster::Cluster& cluster);
    ~QuorumQueue() override;

    bool init(std::string* error);

    EnqueueResult enqueue(const MessagePtr& msg) override;
    std::optional<Delivery> basicGet(bool noAck) override;
    bool addConsumer(const ConsumerPtr& consumer, std::string* error) override;
    void removeConsumer(const std::string& tag, uint64_t connectionId) override;
    void settle(const std::vector<uint64_t>& tags, Settlement how) override;
    void creditAvailable() override {}
    uint32_t purge() override;
    void tick(int64_t nowMs) override;
    void deleted() override;

    size_t messageCount() const override;
    size_t unackedCount() const override;
    size_t consumerCount() const override;
    uint64_t messageBytes() const override;
    Json info() const override;

    // Executes effects produced by the state machine (deliveries to local
    // consumers, dead-lettering on the leader).
    void dispatchEffects();

private:
    std::string consumerKey(const std::string& tag, uint64_t connectionId) const;
    bool propose(const std::string& cmd, std::string* result);
    cluster::QuorumStateMachine::Config configFromArgs() const;

    cluster::Cluster& cluster_;
    std::string groupId_;
    std::unique_ptr<cluster::QuorumStateMachine> sm_;
    std::shared_ptr<cluster::RaftNode> raft_;
    int proposeTimeoutMs_ = 5000;

    mutable std::mutex mu_;
    std::map<std::string, ConsumerPtr> localConsumers_;
    std::mutex dispatchMu_;
    bool deleted_ = false;
};

}  // namespace tq::broker
