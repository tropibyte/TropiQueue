// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include "broker/queue.h"

namespace tq::cluster {
class Cluster;
}

namespace tq::broker {

// Stand-in for a classic queue hosted on another cluster node. Publishes are
// forwarded to the home node; consuming requires connecting to that node.
class RemoteQueue : public QueueBase {
public:
    RemoteQueue(QueueDef def, cluster::Cluster& cluster, int homeNode);

    EnqueueResult enqueue(const MessagePtr& msg) override;
    std::optional<Delivery> basicGet(bool) override { return std::nullopt; }
    bool addConsumer(const ConsumerPtr& consumer, std::string* error) override;
    void removeConsumer(const std::string&, uint64_t) override {}
    void settle(const std::vector<uint64_t>&, Settlement) override {}
    void creditAvailable() override {}
    uint32_t purge() override { return 0; }
    void tick(int64_t) override {}
    void deleted() override {}

    size_t messageCount() const override { return 0; }
    size_t unackedCount() const override { return 0; }
    size_t consumerCount() const override { return 0; }
    uint64_t messageBytes() const override { return 0; }
    Json info() const override;

private:
    cluster::Cluster& cluster_;
    int homeNode_;
};

}  // namespace tq::broker
