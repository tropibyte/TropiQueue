// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/remote_queue.h"

#include "cluster/cluster.h"

namespace tq::broker {

RemoteQueue::RemoteQueue(QueueDef def, cluster::Cluster& cluster, int homeNode)
    : QueueBase(std::move(def)), cluster_(cluster), homeNode_(homeNode) {}

QueueBase::EnqueueResult RemoteQueue::enqueue(const MessagePtr& msg) {
    counters_.published++;
    cluster_.forwardClassicPublish(homeNode_, def_.vhost, def_.name, msg->encode());
    return EnqueueResult::Accepted;
}

bool RemoteQueue::addConsumer(const ConsumerPtr&, std::string* error) {
    if (error) {
        *error = "classic queue '" + def_.name + "' is hosted on node " + std::to_string(homeNode_) +
                 "; connect to that node to consume";
    }
    return false;
}

Json RemoteQueue::info() const {
    Json j = QueueBase::info();
    j["home_node"] = homeNode_;
    j["state"] = "remote";
    return j;
}

}  // namespace tq::broker
