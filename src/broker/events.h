// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Internal events and the firehose tracer.
//
// Events (queue.created, connection.closed, user.authentication.failure, ...)
// are published to the topic exchange amq.rabbitmq.event with the event name
// as routing key and the event details as message headers, so operators can
// bind a queue and watch what happens on the broker.
//
// When tracing is enabled on a vhost, every publish and every delivery is
// copied to amq.rabbitmq.trace with routing key "publish.<exchange>" or
// "deliver.<queue>".
//
// Callers frequently hold vhost or queue locks, so both are delivered from a
// dedicated thread.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "amqp/field_table.h"
#include "broker/message.h"

namespace tq::broker {

class Broker;

constexpr const char* kEventExchange = "amq.rabbitmq.event";
constexpr const char* kTraceExchange = "amq.rabbitmq.trace";

class EventBus {
public:
    explicit EventBus(Broker& broker, size_t maxPending = 10000);
    ~EventBus();
    EventBus(const EventBus&) = delete;
    EventBus& operator=(const EventBus&) = delete;

    void start();
    void stop();

    void setEventsEnabled(bool on) { eventsEnabled_ = on; }
    bool eventsEnabled() const { return eventsEnabled_.load(); }

    // Publishes an event. An empty vhost means the event is not tied to a
    // virtual host (users, authentication, nodes).
    void emit(const std::string& vhost, const std::string& name, amqp::FieldTable details = {});

    // Firehose hooks; no-ops unless tracing is enabled on the vhost.
    void tracePublish(const std::string& vhost, const MessagePtr& msg, const std::string& user,
                      const std::string& connection, uint16_t channel, const std::vector<std::string>& routedQueues);
    void traceDeliver(const std::string& vhost, const std::string& queue, const MessagePtr& msg,
                      const std::string& user, const std::string& connection, uint16_t channel, bool redelivered);

    // Blocks until everything emitted so far has been published (tests).
    void flush();

    uint64_t publishedCount() const { return published_.load(); }
    uint64_t droppedCount() const { return dropped_.load(); }
    size_t pending() const;

private:
    struct Item {
        std::string vhost;  // empty = every vhost
        std::string exchange;
        MessagePtr msg;
    };

    bool tracingEnabled(const std::string& vhost) const;
    void enqueue(Item item);
    void run();
    void publishItem(const Item& item);

    Broker& broker_;
    size_t maxPending_;
    std::atomic<bool> eventsEnabled_{true};
    std::atomic<uint64_t> published_{0};
    std::atomic<uint64_t> dropped_{0};

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::condition_variable idleCv_;
    std::deque<Item> queue_;
    bool busy_ = false;
    bool stopping_ = false;
    std::thread thread_;
};

// Helpers for building event detail tables.
amqp::FieldTable eventDetails(std::initializer_list<std::pair<const char*, std::string>> fields);

}  // namespace tq::broker
