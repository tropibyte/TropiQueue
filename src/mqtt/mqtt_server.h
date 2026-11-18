// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// MQTT 3.1.1 adapter. MQTT clients publish into and subscribe from the
// amq.topic exchange of a configured virtual host, so MQTT and AMQP clients
// can exchange messages.
#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "broker/queue.h"
#include "mqtt/mqtt_codec.h"
#include "net/tcp_server.h"

namespace tq::broker {
class Broker;
class VirtualHost;
}  // namespace tq::broker

namespace tq::mqtt {

struct MqttOptions {
    std::string vhost = "/";
    std::string exchange = "amq.topic";
    bool allowAnonymous = true;
    std::string defaultUser = "guest";
    std::string defaultPass = "guest";
    uint16_t maxInflight = 100;
    bool retainedMessages = true;
};

class MqttConnection;

struct RetainedMessage {
    std::string topic;
    std::string payload;
    uint8_t qos = 0;
};

class MqttServer {
public:
    MqttServer(broker::Broker& broker, MqttOptions options);
    ~MqttServer();

    bool start(const std::string& host, uint16_t port, std::string* error);
    void stop();
    uint16_t port() const;

    broker::Broker& broker() { return broker_; }
    const MqttOptions& options() const { return options_; }

    // Registers the connection for a client id and returns the connection it
    // replaced (which the caller disconnects).
    std::shared_ptr<MqttConnection> registerClient(const std::string& clientId, const std::shared_ptr<MqttConnection>& c);
    void unregisterClient(const std::string& clientId, const MqttConnection* c);
    size_t clientCount() const;

    void storeRetained(const std::string& topic, const std::string& payload, uint8_t qos);
    std::vector<RetainedMessage> matchRetained(const std::string& filter) const;

private:
    broker::Broker& broker_;
    MqttOptions options_;
    std::unique_ptr<net::TcpServer> server_;
    mutable std::mutex mu_;
    std::map<std::string, std::weak_ptr<MqttConnection>> clients_;
    std::map<std::string, RetainedMessage> retained_;
};

class MqttConnection : public std::enable_shared_from_this<MqttConnection> {
public:
    MqttConnection(MqttServer& server, std::shared_ptr<net::TcpConnection> tcp);
    ~MqttConnection();

    void onData(const char* data, size_t len);
    void onClosed();
    void onTick(int64_t nowMs);

    const std::string& clientId() const { return clientId_; }
    const std::string& user() const { return user_; }
    void kick(const std::string& reason);

    // Called by the subscription consumer.
    bool hasCredit() const;
    void deliver(broker::QueueBase& queue, broker::Delivery d, uint8_t maxQos);

private:
    void handle(const Packet& p);
    void onConnect(const Packet& p);
    void onPublish(const Packet& p);
    void onSubscribe(const Packet& p);
    void onUnsubscribe(const Packet& p);
    void onPuback(const Packet& p);
    void publishWill();
    void cleanup(bool graceful);
    bool ensureSubscriptionQueue(uint8_t qos, std::string* error);
    std::string queueName(uint8_t qos) const;
    void send(const std::string& bytes);

    MqttServer& server_;
    std::shared_ptr<net::TcpConnection> tcp_;
    PacketParser parser_;
    bool connected_ = false;
    bool closed_ = false;
    std::string clientId_;
    std::string user_;
    bool tokenAuth_ = false;
    bool cleanSession_ = true;
    uint16_t keepAlive_ = 0;
    std::atomic<int64_t> lastReceiveMs_{0};
    ConnectPacket will_;
    std::shared_ptr<broker::VirtualHost> vhost_;

    mutable std::mutex mu_;
    std::map<std::string, uint8_t> subscriptions_;  // filter -> granted qos
    std::map<uint8_t, broker::QueuePtr> queues_;     // qos -> subscription queue
    std::map<uint8_t, std::shared_ptr<broker::Consumer>> consumers_;
    std::map<uint16_t, std::pair<broker::QueueBase*, uint64_t>> inflight_;  // packet id -> (queue, tag)
    uint16_t nextPacketId_ = 1;
    std::set<uint16_t> incomingQos2_;
};

}  // namespace tq::mqtt
