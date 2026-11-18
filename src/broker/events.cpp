// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/events.h"

#include "broker/broker.h"
#include "common/clock.h"
#include "common/logging.h"

namespace tq::broker {

using amqp::FieldArray;
using amqp::FieldTable;
using amqp::FieldValue;

amqp::FieldTable eventDetails(std::initializer_list<std::pair<const char*, std::string>> fields) {
    FieldTable t;
    for (const auto& [k, v] : fields) t.set(k, FieldValue::longString(v));
    return t;
}

EventBus::EventBus(Broker& broker, size_t maxPending) : broker_(broker), maxPending_(maxPending) {}

EventBus::~EventBus() { stop(); }

void EventBus::start() {
    std::lock_guard<std::mutex> lock(mu_);
    if (thread_.joinable()) return;
    stopping_ = false;
    thread_ = std::thread([this] { run(); });
}

void EventBus::stop() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        stopping_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
    std::lock_guard<std::mutex> lock(mu_);
    queue_.clear();
    idleCv_.notify_all();
}

size_t EventBus::pending() const {
    std::lock_guard<std::mutex> lock(mu_);
    return queue_.size();
}

void EventBus::flush() {
    std::unique_lock<std::mutex> lock(mu_);
    idleCv_.wait_for(lock, std::chrono::seconds(5), [this] { return (queue_.empty() && !busy_) || stopping_; });
}

void EventBus::enqueue(Item item) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (stopping_ || !thread_.joinable()) return;
        if (queue_.size() >= maxPending_) {
            dropped_++;
            return;
        }
        queue_.push_back(std::move(item));
    }
    cv_.notify_one();
}

void EventBus::run() {
    std::unique_lock<std::mutex> lock(mu_);
    for (;;) {
        cv_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
        if (stopping_) return;
        Item item = std::move(queue_.front());
        queue_.pop_front();
        busy_ = true;
        lock.unlock();
        try {
            publishItem(item);
        } catch (const std::exception& e) {
            LOG_WARN("events") << "cannot publish to " << item.exchange << ": " << e.what();
        }
        lock.lock();
        busy_ = false;
        if (queue_.empty()) idleCv_.notify_all();
    }
}

void EventBus::publishItem(const Item& item) {
    if (item.vhost.empty()) {
        for (const auto& vh : broker_.vhosts()) {
            vh->publish(item.exchange, item.msg);
            published_++;
        }
        return;
    }
    auto vh = broker_.vhost(item.vhost);
    if (!vh) return;
    vh->publish(item.exchange, item.msg);
    published_++;
}

void EventBus::emit(const std::string& vhost, const std::string& name, amqp::FieldTable details) {
    if (!eventsEnabled_) return;
    details.set("timestamp_in_ms", FieldValue::int64(wallClockMs()));
    if (!vhost.empty() && !details.has("vhost")) details.set("vhost", FieldValue::longString(vhost));
    details.set("node", FieldValue::longString(broker_.nodeName()));

    amqp::BasicProperties props;
    props.headers = std::move(details);
    props.timestamp = static_cast<uint64_t>(wallClockMs() / 1000);
    props.contentType = "application/octet-stream";
    props.deliveryMode = 2;

    auto msg = std::make_shared<Message>();
    msg->id = nextMessageId();
    msg->exchange = kEventExchange;
    msg->routingKey = name;
    msg->props = std::move(props);
    msg->publishedAtMs = wallClockMs();
    msg->publisherVhost = vhost;
    enqueue(Item{vhost, kEventExchange, std::move(msg)});
}

bool EventBus::tracingEnabled(const std::string& vhost) const {
    auto vh = broker_.vhost(vhost);
    return vh && vh->tracing();
}

void EventBus::tracePublish(const std::string& vhost, const MessagePtr& msg, const std::string& user,
                            const std::string& connection, uint16_t channel,
                            const std::vector<std::string>& routedQueues) {
    if (msg->exchange == kTraceExchange || !tracingEnabled(vhost)) return;
    FieldTable h;
    h.set("exchange_name", FieldValue::longString(msg->exchange));
    FieldArray keys;
    keys.push_back(FieldValue::longString(msg->routingKey));
    h.set("routing_keys", FieldValue::fromArray(std::move(keys)));
    h.set("properties", FieldValue::fromTable(FieldTable::fromJson(msg->props.toJson())));
    h.set("node", FieldValue::longString(broker_.nodeName()));
    h.set("vhost", FieldValue::longString(vhost));
    h.set("user", FieldValue::longString(user));
    h.set("connection", FieldValue::longString(connection));
    h.set("channel", FieldValue::int32(channel));
    FieldArray routed;
    for (const auto& q : routedQueues) routed.push_back(FieldValue::longString(q));
    h.set("routed_queues", FieldValue::fromArray(std::move(routed)));

    auto t = std::make_shared<Message>();
    t->id = nextMessageId();
    t->exchange = kTraceExchange;
    t->routingKey = "publish." + msg->exchange;
    t->props.headers = std::move(h);
    t->body = msg->body;
    t->publishedAtMs = wallClockMs();
    t->publisherVhost = vhost;
    enqueue(Item{vhost, kTraceExchange, std::move(t)});
}

void EventBus::traceDeliver(const std::string& vhost, const std::string& queue, const MessagePtr& msg,
                            const std::string& user, const std::string& connection, uint16_t channel,
                            bool redelivered) {
    if (!tracingEnabled(vhost)) return;
    FieldTable h;
    h.set("exchange_name", FieldValue::longString(msg->exchange));
    FieldArray keys;
    keys.push_back(FieldValue::longString(msg->routingKey));
    h.set("routing_keys", FieldValue::fromArray(std::move(keys)));
    h.set("properties", FieldValue::fromTable(FieldTable::fromJson(msg->props.toJson())));
    h.set("node", FieldValue::longString(broker_.nodeName()));
    h.set("vhost", FieldValue::longString(vhost));
    h.set("user", FieldValue::longString(user));
    h.set("connection", FieldValue::longString(connection));
    h.set("channel", FieldValue::int32(channel));
    h.set("redelivered", FieldValue::boolean(redelivered));

    auto t = std::make_shared<Message>();
    t->id = nextMessageId();
    t->exchange = kTraceExchange;
    t->routingKey = "deliver." + queue;
    t->props.headers = std::move(h);
    t->body = msg->body;
    t->publishedAtMs = wallClockMs();
    t->publisherVhost = vhost;
    enqueue(Item{vhost, kTraceExchange, std::move(t)});
}

}  // namespace tq::broker
