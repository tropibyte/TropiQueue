// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Automatic connection and topology recovery for the client library.
//
// A RecoveringConnection wraps client::Connection. When the connection is
// lost (network failure, broker restart, forced close from the management
// UI) it reconnects with the same parameters, reopens every channel, applies
// qos and confirm mode again and - unless disabled - re-declares the
// exchanges, queues and bindings and restarts the consumers that were
// created through it.
//
// Delivery tags are channel-scoped, so after a recovery the broker numbers
// deliveries from 1 again. RecoveringChannel hides this by adding an offset
// to the tags it hands to consumers; acknowledgements for deliveries made
// before the recovery are dropped (the broker requeued those messages when
// the old connection died).
#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "client/channel.h"
#include "client/connection.h"

namespace tq::client {

class RecoveringConnection;

struct RecoveryOptions {
    int networkRecoveryIntervalMs = 5000;
    int maxAttempts = -1;  // -1 = retry forever
    bool topologyRecovery = true;
};

class RecoveringChannel : public std::enable_shared_from_this<RecoveringChannel> {
public:
    RecoveringChannel(std::weak_ptr<RecoveringConnection> owner, std::shared_ptr<Channel> channel);

    bool isOpen() const;

    void declareExchange(const std::string& name, const std::string& type, bool durable = true,
                         bool autoDelete = false, bool internal = false, const amqp::FieldTable& args = {});
    void deleteExchange(const std::string& name, bool ifUnused = false);
    void bindExchange(const std::string& destination, const std::string& source, const std::string& routingKey,
                      const amqp::FieldTable& args = {});
    QueueInfo declareQueue(const std::string& name, bool durable = true, bool exclusive = false,
                           bool autoDelete = false, const amqp::FieldTable& args = {});
    uint32_t deleteQueue(const std::string& name, bool ifUnused = false, bool ifEmpty = false);
    void bindQueue(const std::string& queue, const std::string& exchange, const std::string& routingKey,
                   const amqp::FieldTable& args = {});
    void unbindQueue(const std::string& queue, const std::string& exchange, const std::string& routingKey,
                     const amqp::FieldTable& args = {});

    void qos(uint16_t prefetchCount, bool global = false);
    void publish(const std::string& exchange, const std::string& routingKey, const std::string& body,
                 const amqp::BasicProperties& props = {}, bool mandatory = false);
    std::string consume(const std::string& queue, Channel::ConsumerCallback cb, bool noAck = false,
                        const std::string& tag = "", bool exclusive = false, const amqp::FieldTable& args = {});
    void cancel(const std::string& consumerTag);
    void ack(uint64_t deliveryTag, bool multiple = false);
    void nack(uint64_t deliveryTag, bool multiple = false, bool requeue = true);
    void reject(uint64_t deliveryTag, bool requeue = true);

    void confirmSelect();
    bool waitForConfirms(int timeoutMs);

    void close();

    // The channel currently in use (changes after a recovery).
    std::shared_ptr<Channel> current() const;

private:
    friend class RecoveringConnection;

    struct ConsumerRecord {
        std::string queue;
        Channel::ConsumerCallback callback;
        bool noAck = false;
        bool exclusive = false;
        amqp::FieldTable arguments;
    };

    // Replaces the underlying channel after the connection was recovered.
    void recoverOn(const std::shared_ptr<Connection>& conn);
    void recoverConsumers();
    Channel::ConsumerCallback wrap(Channel::ConsumerCallback cb);
    std::optional<uint64_t> toBrokerTag(uint64_t tag) const;

    std::weak_ptr<RecoveringConnection> owner_;
    mutable std::mutex mu_;
    std::shared_ptr<Channel> channel_;
    bool closedByUser_ = false;
    std::optional<std::pair<uint16_t, bool>> qos_;
    bool confirmMode_ = false;
    uint64_t tagOffset_ = 0;          // added to broker tags handed to consumers
    uint64_t highestTagSeen_ = 0;     // highest offset tag delivered so far
    std::map<std::string, ConsumerRecord> consumers_;
};

class RecoveringConnection : public std::enable_shared_from_this<RecoveringConnection> {
public:
    static std::shared_ptr<RecoveringConnection> open(const ConnectionParams& params, RecoveryOptions options = {},
                                                      std::string* error = nullptr);
    ~RecoveringConnection();

    std::shared_ptr<RecoveringChannel> openChannel();
    void close();
    bool isOpen() const;

    // Callbacks run on the recovery thread.
    void onConnectionLost(std::function<void(const std::string& reason)> cb);
    void onRecovered(std::function<void()> cb);
    void onRecoveryFailed(std::function<void(const std::string& error)> cb);

    uint64_t recoveryCount() const { return recoveries_.load(); }
    std::shared_ptr<Connection> current() const;

private:
    friend class RecoveringChannel;

    struct ExchangeRecord {
        std::string type;
        bool durable = true;
        bool autoDelete = false;
        bool internal = false;
        amqp::FieldTable arguments;
    };
    struct QueueRecord {
        std::string name;
        bool durable = true;
        bool exclusive = false;
        bool autoDelete = false;
        bool serverNamed = false;
        amqp::FieldTable arguments;
    };
    struct BindingRecord {
        bool toExchange = false;
        std::string destination;
        std::string source;
        std::string routingKey;
        amqp::FieldTable arguments;
        bool operator==(const BindingRecord& o) const {
            return toExchange == o.toExchange && destination == o.destination && source == o.source &&
                   routingKey == o.routingKey && arguments == o.arguments;
        }
    };

    RecoveringConnection(ConnectionParams params, RecoveryOptions options);
    void monitor();
    bool recover(std::string* error);
    void recoverTopology(Channel& ch);

    // Topology bookkeeping, called by RecoveringChannel.
    void recordExchange(const std::string& name, ExchangeRecord r);
    void forgetExchange(const std::string& name);
    void recordQueue(QueueRecord r);
    void forgetQueue(const std::string& name);
    void recordBinding(BindingRecord r);
    void forgetBinding(const BindingRecord& r);

    ConnectionParams params_;
    RecoveryOptions options_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::shared_ptr<Connection> conn_;
    std::vector<std::weak_ptr<RecoveringChannel>> channels_;
    std::map<std::string, ExchangeRecord> exchanges_;
    std::map<std::string, QueueRecord> queues_;
    std::vector<BindingRecord> bindings_;

    std::function<void(const std::string&)> onLost_;
    std::function<void()> onRecovered_;
    std::function<void(const std::string&)> onFailed_;

    std::atomic<bool> closing_{false};
    std::atomic<bool> recovering_{false};
    std::atomic<uint64_t> recoveries_{0};
    std::thread monitor_;
};

}  // namespace tq::client
