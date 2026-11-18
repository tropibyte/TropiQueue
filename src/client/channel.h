// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "amqp/methods.h"
#include "amqp/properties.h"

namespace tq::client {

class Connection;

struct Delivery {
    std::string consumerTag;
    uint64_t deliveryTag = 0;
    bool redelivered = false;
    std::string exchange;
    std::string routingKey;
    uint32_t messageCount = 0;  // basic.get only
    amqp::BasicProperties properties;
    std::string body;
};

struct ReturnedMessage {
    uint16_t replyCode = 0;
    std::string replyText;
    std::string exchange;
    std::string routingKey;
    amqp::BasicProperties properties;
    std::string body;
};

struct QueueInfo {
    std::string name;
    uint32_t messageCount = 0;
    uint32_t consumerCount = 0;
};

class Channel : public std::enable_shared_from_this<Channel> {
public:
    using ConsumerCallback = std::function<void(const Delivery&)>;
    using ReturnCallback = std::function<void(const ReturnedMessage&)>;

    Channel(std::weak_ptr<Connection> conn, uint16_t id);

    uint16_t id() const { return id_; }
    bool isOpen() const;
    std::string closeReason() const;
    uint16_t closeCode() const;

    // ----- topology
    void declareExchange(const std::string& name, const std::string& type, bool durable = true,
                         bool autoDelete = false, bool internal = false, const amqp::FieldTable& args = {});
    void deleteExchange(const std::string& name, bool ifUnused = false);
    void bindExchange(const std::string& destination, const std::string& source, const std::string& routingKey,
                      const amqp::FieldTable& args = {});
    QueueInfo declareQueue(const std::string& name, bool durable = true, bool exclusive = false,
                           bool autoDelete = false, const amqp::FieldTable& args = {});
    QueueInfo declareQueuePassive(const std::string& name);
    void bindQueue(const std::string& queue, const std::string& exchange, const std::string& routingKey,
                   const amqp::FieldTable& args = {});
    void unbindQueue(const std::string& queue, const std::string& exchange, const std::string& routingKey,
                     const amqp::FieldTable& args = {});
    uint32_t purgeQueue(const std::string& name);
    uint32_t deleteQueue(const std::string& name, bool ifUnused = false, bool ifEmpty = false);

    // ----- messaging
    void qos(uint16_t prefetchCount, bool global = false);
    void publish(const std::string& exchange, const std::string& routingKey, const std::string& body,
                 const amqp::BasicProperties& props = {}, bool mandatory = false);
    std::string consume(const std::string& queue, ConsumerCallback cb, bool noAck = false,
                        const std::string& tag = "", bool exclusive = false, const amqp::FieldTable& args = {});
    void cancel(const std::string& consumerTag);
    std::optional<Delivery> get(const std::string& queue, bool noAck = false);
    void ack(uint64_t deliveryTag, bool multiple = false);
    void nack(uint64_t deliveryTag, bool multiple = false, bool requeue = true);
    void reject(uint64_t deliveryTag, bool requeue = true);
    void recover(bool requeue = true);
    void onReturn(ReturnCallback cb);

    // ----- publisher confirms and transactions
    void confirmSelect();
    // Waits until every outstanding publish has been confirmed. Returns false
    // on timeout or if any message was nacked.
    bool waitForConfirms(int timeoutMs);
    uint64_t unconfirmedCount() const;
    void txSelect();
    void txCommit();
    void txRollback();

    void close();

    // Called by the connection's reader thread.
    void handleFrame(uint8_t type, const std::string& payload);
    void connectionClosed(uint16_t code, const std::string& text);

private:
    amqp::Method rpc(const amqp::Method& request, uint16_t expectedClass, uint16_t expectedMethod);
    void send(const amqp::Method& m);
    void throwIfClosed() const;
    void deliverAssembled();

    std::weak_ptr<Connection> conn_;
    uint16_t id_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<amqp::Method> replies_;
    bool open_ = true;
    uint16_t closeCode_ = 0;
    std::string closeReason_;

    std::map<std::string, ConsumerCallback> consumers_;
    ReturnCallback returnCallback_;

    // content assembly
    std::optional<amqp::Method> contentMethod_;
    std::optional<amqp::BasicProperties> contentProps_;
    uint64_t contentSize_ = 0;
    std::string contentBody_;
    std::deque<Delivery> gets_;
    bool getEmpty_ = false;

    // confirms
    bool confirmMode_ = false;
    uint64_t nextPublishSeq_ = 1;
    std::map<uint64_t, bool> pendingConfirms_;  // seq -> nacked
    bool anyNacked_ = false;
};

}  // namespace tq::client
