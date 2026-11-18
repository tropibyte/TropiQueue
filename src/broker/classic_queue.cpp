// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/classic_queue.h"

#include <algorithm>

#include "common/clock.h"
#include "common/logging.h"

namespace tq::broker {

ClassicQueue::ClassicQueue(QueueDef def, std::unique_ptr<QueueStorage> storage)
    : QueueBase(std::move(def)), storage_(std::move(storage)) {
    levels_.resize(1);
    reconfigure();
}

ClassicQueue::~ClassicQueue() = default;

void ClassicQueue::reconfigure() {
    std::lock_guard<std::mutex> lock(mu_);
    const amqp::FieldTable& a = effectiveArgs_;
    messageTtlMs_ = a.getInt("x-message-ttl").value_or(-1);
    maxLength_ = a.getInt("x-max-length").value_or(-1);
    maxLengthBytes_ = a.getInt("x-max-length-bytes").value_or(-1);
    std::string overflow = a.getString("x-overflow").value_or("drop-head");
    if (overflow == "reject-publish") overflow_ = OverflowMode::RejectPublish;
    else if (overflow == "reject-publish-dlx") overflow_ = OverflowMode::RejectPublishDlx;
    else overflow_ = OverflowMode::DropHead;
    singleActiveConsumer_ = a.getBool("x-single-active-consumer", false);

    int64_t maxPriority = a.getInt("x-max-priority").value_or(0);
    if (maxPriority < 0) maxPriority = 0;
    if (maxPriority != maxPriority_) {
        std::deque<Entry> all;
        for (auto& level : levels_) {
            for (auto& e : level) all.push_back(std::move(e));
        }
        maxPriority_ = static_cast<int>(maxPriority);
        levels_.clear();
        levels_.resize(static_cast<size_t>(maxPriority_) + 1);
        for (auto& e : all) levels_[levelFor(*e.msg)].push_back(std::move(e));
    }
}

size_t ClassicQueue::levelFor(const Message& msg) const {
    if (maxPriority_ == 0) return 0;
    return msg.priority();
}

size_t ClassicQueue::readyCountLocked() const {
    size_t n = 0;
    for (const auto& level : levels_) n += level.size();
    return n;
}

bool ClassicQueue::overLimitLocked() const {
    if (maxLength_ >= 0 && static_cast<int64_t>(readyCountLocked()) >= maxLength_) return true;
    if (maxLengthBytes_ >= 0 && static_cast<int64_t>(bytes_) > maxLengthBytes_) return true;
    return false;
}

int64_t ClassicQueue::expiryFor(const Message& msg, int64_t nowMs) const {
    int64_t ttl = messageTtlMs_;
    if (auto perMessage = msg.ttlMs()) ttl = ttl < 0 ? *perMessage : std::min(ttl, *perMessage);
    if (ttl < 0) return 0;
    return nowMs + ttl;
}

void ClassicQueue::releaseEntryLocked(const Entry& e) {
    bytes_ -= std::min<uint64_t>(bytes_, e.msg->size());
    if (storage_ && e.msg->persistent()) storage_->recordAck(e.tag);
}

void ClassicQueue::deadLetterLocked(const Entry& e, const std::string& reason) {
    if (!deadLetterSink_ || !effectiveArgs_.has("x-dead-letter-exchange")) return;
    counters_.deadLettered++;
    pendingDeadLetters_.emplace_back(e.msg, reason);
}

void ClassicQueue::flushDeadLetters() {
    std::vector<std::pair<MessagePtr, std::string>> batch;
    {
        std::lock_guard<std::mutex> lock(mu_);
        batch.swap(pendingDeadLetters_);
    }
    for (auto& [msg, reason] : batch) deadLetterSink_->deadLetter(*this, msg, reason);
}

void ClassicQueue::dropHeadLocked() {
    for (auto& level : levels_) {
        if (level.empty()) continue;
        Entry e = std::move(level.front());
        level.pop_front();
        releaseEntryLocked(e);
        counters_.dropped++;
        return;
    }
}

std::optional<ClassicQueue::Entry> ClassicQueue::popNextLocked(int64_t nowMs) {
    for (auto it = levels_.rbegin(); it != levels_.rend(); ++it) {
        auto& level = *it;
        while (!level.empty()) {
            Entry e = std::move(level.front());
            level.pop_front();
            if (e.expiresAt != 0 && e.expiresAt <= nowMs) {
                counters_.expired++;
                deadLetterLocked(e, "expired");
                releaseEntryLocked(e);
                continue;
            }
            return e;
        }
    }
    return std::nullopt;
}

ConsumerPtr ClassicQueue::pickConsumerLocked() {
    if (consumers_.empty()) return nullptr;
    if (singleActiveConsumer_) {
        if (!activeConsumer_) return nullptr;
        return activeConsumer_->hasCredit() ? activeConsumer_ : nullptr;
    }
    int top = consumers_.front()->priority();
    for (const auto& c : consumers_) top = std::max(top, c->priority());
    size_t n = consumers_.size();
    for (size_t i = 0; i < n; ++i) {
        size_t idx = (rrIndex_ + i) % n;
        const ConsumerPtr& c = consumers_[idx];
        if (c->priority() != top) continue;
        if (c->hasCredit()) {
            rrIndex_ = (idx + 1) % n;
            return c;
        }
    }
    return nullptr;
}

void ClassicQueue::tryDeliverLocked() {
    if (delivering_ || deleted_) return;
    delivering_ = true;
    int64_t now = wallClockMs();
    while (readyCountLocked() > 0) {
        ConsumerPtr c = pickConsumerLocked();
        if (!c) break;
        auto e = popNextLocked(now);
        if (!e) break;
        e->deliveryCount++;
        Delivery d;
        d.msg = e->msg;
        d.queueTag = e->tag;
        d.redelivered = e->redelivered;
        d.deliveryCount = e->deliveryCount;
        d.remaining = static_cast<uint32_t>(readyCountLocked());
        counters_.delivered++;
        if (e->redelivered) counters_.redelivered++;
        if (c->noAck()) {
            releaseEntryLocked(*e);
            counters_.acked++;
        } else {
            if (storage_ && e->msg->persistent()) storage_->recordDelivered(e->tag);
            unacked_[e->tag] = *e;
        }
        c->deliver(*this, std::move(d));
    }
    delivering_ = false;
}

QueueBase::EnqueueResult ClassicQueue::enqueue(const MessagePtr& msg) {
    std::unique_lock<std::mutex> lock(mu_);
    if (deleted_) return EnqueueResult::Rejected;
    counters_.published++;
    bytes_ += msg->size();
    if (overLimitLocked()) {
        if (overflow_ != OverflowMode::DropHead) {
            counters_.dropped++;
            if (overflow_ == OverflowMode::RejectPublishDlx) deadLetterLocked(Entry{msg, 0, 0, false, 0}, "maxlen");
            lock.unlock();
            flushDeadLetters();
            return EnqueueResult::Rejected;
        }
        while (overLimitLocked() && readyCountLocked() > 0) dropHeadLocked();
    }
    Entry e;
    e.msg = msg;
    e.tag = nextTag_++;
    e.expiresAt = expiryFor(*msg, wallClockMs());
    if (storage_ && msg->persistent()) storage_->recordPublish(e.tag, msg);
    levels_[levelFor(*msg)].push_back(std::move(e));
    tryDeliverLocked();
    lock.unlock();
    flushDeadLetters();
    return EnqueueResult::Accepted;
}

std::optional<Delivery> ClassicQueue::basicGet(bool noAck) {
    std::unique_lock<std::mutex> lock(mu_);
    auto e = popNextLocked(wallClockMs());
    std::optional<Delivery> out;
    if (e) {
        e->deliveryCount++;
        Delivery d;
        d.msg = e->msg;
        d.queueTag = e->tag;
        d.redelivered = e->redelivered;
        d.deliveryCount = e->deliveryCount;
        d.remaining = static_cast<uint32_t>(readyCountLocked());
        counters_.delivered++;
        if (noAck) {
            releaseEntryLocked(*e);
            counters_.acked++;
        } else {
            if (storage_ && e->msg->persistent()) storage_->recordDelivered(e->tag);
            unacked_[e->tag] = *e;
        }
        out = std::move(d);
    }
    lock.unlock();
    flushDeadLetters();
    return out;
}

bool ClassicQueue::addConsumer(const ConsumerPtr& consumer, std::string* error) {
    std::unique_lock<std::mutex> lock(mu_);
    if (deleted_) {
        if (error) *error = "queue '" + def_.name + "' has been deleted";
        return false;
    }
    for (const auto& c : consumers_) {
        if (c->exclusive()) {
            if (error) *error = "queue '" + def_.name + "' in vhost '" + def_.vhost + "' has an exclusive consumer";
            return false;
        }
    }
    if (consumer->exclusive() && !consumers_.empty()) {
        if (error) *error = "queue '" + def_.name + "' in vhost '" + def_.vhost + "' in exclusive use";
        return false;
    }
    consumers_.push_back(consumer);
    if (singleActiveConsumer_ && !activeConsumer_) activeConsumer_ = consumer;
    tryDeliverLocked();
    lock.unlock();
    flushDeadLetters();
    return true;
}

void ClassicQueue::removeConsumer(const std::string& tag, uint64_t connectionId) {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto it = consumers_.begin(); it != consumers_.end(); ++it) {
        if ((*it)->tag() == tag && (*it)->connectionId() == connectionId) {
            if (activeConsumer_ == *it) activeConsumer_.reset();
            consumers_.erase(it);
            break;
        }
    }
    if (rrIndex_ >= consumers_.size()) rrIndex_ = 0;
}

void ClassicQueue::settle(const std::vector<uint64_t>& tags, Settlement how) {
    std::unique_lock<std::mutex> lock(mu_);
    for (uint64_t tag : tags) {
        auto it = unacked_.find(tag);
        if (it == unacked_.end()) continue;
        Entry e = std::move(it->second);
        unacked_.erase(it);
        switch (how) {
            case Settlement::Ack:
                releaseEntryLocked(e);
                counters_.acked++;
                break;
            case Settlement::Requeue:
                levels_[levelFor(*e.msg)].push_back(std::move(e));
                break;
            case Settlement::Reject:
                deadLetterLocked(e, "rejected");
                releaseEntryLocked(e);
                break;
        }
    }
    tryDeliverLocked();
    lock.unlock();
    flushDeadLetters();
}

void ClassicQueue::creditAvailable() {
    std::unique_lock<std::mutex> lock(mu_);
    tryDeliverLocked();
    lock.unlock();
    flushDeadLetters();
}

uint32_t ClassicQueue::purge() {
    std::lock_guard<std::mutex> lock(mu_);
    uint32_t n = 0;
    for (auto& level : levels_) {
        for (auto& e : level) {
            releaseEntryLocked(e);
            ++n;
        }
        level.clear();
    }
    return n;
}

void ClassicQueue::tick(int64_t nowMs) {
    std::unique_lock<std::mutex> lock(mu_);
    if (deleted_) return;
    for (auto& level : levels_) {
        while (!level.empty() && level.front().expiresAt != 0 && level.front().expiresAt <= nowMs) {
            Entry e = std::move(level.front());
            level.pop_front();
            counters_.expired++;
            deadLetterLocked(e, "expired");
            releaseEntryLocked(e);
        }
    }
    if (storage_) storage_->flush();
    lock.unlock();
    flushDeadLetters();
}

void ClassicQueue::deleted() {
    std::lock_guard<std::mutex> lock(mu_);
    deleted_ = true;
    consumers_.clear();
    activeConsumer_.reset();
    for (auto& level : levels_) level.clear();
    unacked_.clear();
    bytes_ = 0;
    if (storage_) storage_->destroy();
}

void ClassicQueue::recover() {
    if (!storage_) return;
    std::lock_guard<std::mutex> lock(mu_);
    int64_t now = wallClockMs();
    for (auto& r : storage_->recover()) {
        Entry e;
        e.msg = r.msg;
        e.tag = r.tag;
        e.redelivered = r.delivered;
        e.expiresAt = expiryFor(*r.msg, now);
        bytes_ += r.msg->size();
        nextTag_ = std::max(nextTag_, r.tag + 1);
        levels_[levelFor(*r.msg)].push_back(std::move(e));
    }
}

size_t ClassicQueue::messageCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return readyCountLocked();
}

size_t ClassicQueue::unackedCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return unacked_.size();
}

size_t ClassicQueue::consumerCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return consumers_.size();
}

uint64_t ClassicQueue::messageBytes() const {
    std::lock_guard<std::mutex> lock(mu_);
    return bytes_;
}

Json ClassicQueue::info() const {
    Json j = QueueBase::info();
    std::lock_guard<std::mutex> lock(mu_);
    Json consumers = Json::array();
    for (const auto& c : consumers_) {
        Json cj = Json::object();
        cj["consumer_tag"] = c->tag();
        cj["ack_required"] = !c->noAck();
        cj["exclusive"] = c->exclusive();
        cj["priority"] = c->priority();
        cj["active"] = !singleActiveConsumer_ || activeConsumer_ == c;
        consumers.push(cj);
    }
    j["consumer_details"] = consumers;
    j["single_active_consumer"] = singleActiveConsumer_;
    j["max_priority"] = maxPriority_;
    return j;
}

}  // namespace tq::broker
