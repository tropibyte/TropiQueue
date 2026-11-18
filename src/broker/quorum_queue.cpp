// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/quorum_queue.h"

#include "cluster/cluster.h"
#include "common/bytes.h"
#include "common/clock.h"
#include "common/logging.h"
#include "common/strings.h"

namespace tq::broker {

QuorumQueue::QuorumQueue(QueueDef def, cluster::Cluster& cluster)
    : QueueBase(std::move(def)), cluster_(cluster), groupId_(cluster::quorumGroupId(def_.vhost, def_.name)) {}

QuorumQueue::~QuorumQueue() {
    if (!deleted_) cluster_.removeGroup(groupId_, false);
}

cluster::QuorumStateMachine::Config QuorumQueue::configFromArgs() const {
    cluster::QuorumStateMachine::Config c;
    const amqp::FieldTable& a = effectiveArgs_;
    c.deliveryLimit = a.getInt("x-delivery-limit").value_or(-1);
    c.maxLength = a.getInt("x-max-length").value_or(-1);
    c.maxLengthBytes = a.getInt("x-max-length-bytes").value_or(-1);
    c.rejectPublish = a.getString("x-overflow").value_or("drop-head") == "reject-publish";
    c.messageTtlMs = a.getInt("x-message-ttl").value_or(-1);
    c.deadLetter = a.has("x-dead-letter-exchange");
    return c;
}

bool QuorumQueue::init(std::string* error) {
    sm_ = std::make_unique<cluster::QuorumStateMachine>(configFromArgs());
    std::vector<int> members = def_.replicas;
    if (members.empty()) members = cluster_.pickReplicas(0);
    cluster::Cluster::GroupHooks hooks;
    std::weak_ptr<QueueBase> weak = weak_from_this();
    hooks.afterApply = [weak] {
        if (auto self = weak.lock()) static_cast<QuorumQueue*>(self.get())->dispatchEffects();
    };
    raft_ = cluster_.createGroup(groupId_, members, *sm_, hooks, error);
    if (!raft_) {
        if (error && error->empty()) *error = "this node is not a member of quorum queue '" + def_.name + "'";
        return false;
    }
    return true;
}

std::string QuorumQueue::consumerKey(const std::string& tag, uint64_t connectionId) const {
    return std::to_string(cluster_.selfId()) + "/" + std::to_string(connectionId) + "/" + tag;
}

bool QuorumQueue::propose(const std::string& cmd, std::string* result) {
    bool ok = cluster_.propose(groupId_, cmd, proposeTimeoutMs_, result);
    dispatchEffects();
    return ok;
}

void QuorumQueue::dispatchEffects() {
    std::lock_guard<std::mutex> dispatchLock(dispatchMu_);
    if (!sm_) return;
    auto effects = sm_->takeEffects();
    std::string prefix = std::to_string(cluster_.selfId()) + "/";
    for (auto& e : effects) {
        if (e.kind == cluster::QuorumStateMachine::Effect::Kind::DeadLetter) {
            if (raft_ && raft_->isLeader() && deadLetterSink_) {
                counters_.deadLettered++;
                deadLetterSink_->deadLetter(*this, e.msg, e.reason);
            }
            continue;
        }
        if (!str::startsWith(e.consumer, prefix)) continue;
        ConsumerPtr c;
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = localConsumers_.find(e.consumer);
            if (it != localConsumers_.end()) c = it->second;
        }
        if (!c) continue;
        Delivery d;
        d.msg = e.msg;
        d.queueTag = e.id;
        d.redelivered = e.deliveryCount > 0;
        d.deliveryCount = e.deliveryCount + 1;
        counters_.delivered++;
        if (d.redelivered) counters_.redelivered++;
        c->deliver(*this, std::move(d));
    }
}

QueueBase::EnqueueResult QuorumQueue::enqueue(const MessagePtr& msg) {
    counters_.published++;
    std::string result;
    if (!propose(cluster::QuorumStateMachine::enqueueCommand(*msg, wallClockMs()), &result)) {
        LOG_WARN("quorum") << "publish to '" << def_.name << "' timed out waiting for a quorum";
        return EnqueueResult::Rejected;
    }
    return result == "R" ? EnqueueResult::Rejected : EnqueueResult::Accepted;
}

std::optional<Delivery> QuorumQueue::basicGet(bool noAck) {
    std::string result;
    std::string getter = std::to_string(cluster_.selfId()) + "/" + std::to_string(monotonicMs());
    if (!propose(cluster::QuorumStateMachine::getCommand(noAck, getter), &result) || result.empty()) return std::nullopt;
    ByteReader r(result);
    Delivery d;
    d.queueTag = r.u64();
    uint32_t returns = r.u32();
    d.remaining = r.u32();
    d.msg = Message::decode(r.longString());
    d.redelivered = returns > 0;
    d.deliveryCount = returns + 1;
    counters_.delivered++;
    return d;
}

bool QuorumQueue::addConsumer(const ConsumerPtr& consumer, std::string* error) {
    std::string key = consumerKey(consumer->tag(), consumer->connectionId());
    {
        std::lock_guard<std::mutex> lock(mu_);
        localConsumers_[key] = consumer;
    }
    if (!propose(cluster::QuorumStateMachine::checkoutCommand(key, consumer->prefetchCount(), consumer->noAck()),
                 nullptr)) {
        std::lock_guard<std::mutex> lock(mu_);
        localConsumers_.erase(key);
        if (error) *error = "quorum queue '" + def_.name + "' is unavailable (no leader)";
        return false;
    }
    return true;
}

void QuorumQueue::removeConsumer(const std::string& tag, uint64_t connectionId) {
    std::string key = consumerKey(tag, connectionId);
    {
        std::lock_guard<std::mutex> lock(mu_);
        localConsumers_.erase(key);
    }
    propose(cluster::QuorumStateMachine::cancelCommand(key), nullptr);
}

void QuorumQueue::settle(const std::vector<uint64_t>& tags, Settlement how) {
    uint8_t op = how == Settlement::Ack ? 0 : how == Settlement::Requeue ? 1 : 2;
    if (op == 0) counters_.acked += tags.size();
    propose(cluster::QuorumStateMachine::settleCommand(tags, op), nullptr);
}

uint32_t QuorumQueue::purge() {
    std::string result;
    if (!propose(cluster::QuorumStateMachine::purgeCommand(), &result)) return 0;
    return static_cast<uint32_t>(str::parseUint(result).value_or(0));
}

void QuorumQueue::tick(int64_t nowMs) {
    if (!raft_ || !raft_->isLeader()) return;
    bool hasTtl = effectiveArgs_.has("x-message-ttl") || messageCount() > 0;
    if (hasTtl) propose(cluster::QuorumStateMachine::tickCommand(nowMs), nullptr);
}

void QuorumQueue::deleted() {
    deleted_ = true;
    {
        std::lock_guard<std::mutex> lock(mu_);
        localConsumers_.clear();
    }
    cluster_.removeGroup(groupId_, true);
    raft_.reset();
}

size_t QuorumQueue::messageCount() const { return sm_ ? sm_->readyCount() : 0; }
size_t QuorumQueue::unackedCount() const { return sm_ ? sm_->checkedOutCount() : 0; }
size_t QuorumQueue::consumerCount() const { return sm_ ? sm_->consumerCount() : 0; }
uint64_t QuorumQueue::messageBytes() const { return sm_ ? sm_->bytes() : 0; }

Json QuorumQueue::info() const {
    Json j = QueueBase::info();
    if (raft_) {
        j["leader"] = raft_->leaderId();
        Json members = Json::array();
        for (int m : raft_->members()) members.push(m);
        j["members"] = members;
        j["raft_term"] = static_cast<int64_t>(raft_->currentTerm());
        j["raft_commit_index"] = static_cast<int64_t>(raft_->commitIndex());
    }
    return j;
}

}  // namespace tq::broker
