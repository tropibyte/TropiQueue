// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// STOMP 1.0-1.2 adapter using RabbitMQ-style destinations:
//   /queue/<name>, /amq/queue/<name>, /topic/<key>, /exchange/<name>[/<key>]
#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "broker/queue.h"
#include "net/tcp_server.h"

namespace tq::broker {
class Broker;
class VirtualHost;
}  // namespace tq::broker

namespace tq::stomp {

struct StompFrame {
    std::string command;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;

    std::optional<std::string> header(const std::string& name) const;
    void set(const std::string& name, const std::string& value);
    std::string encode() const;
};

// Incremental STOMP frame parser.
class StompParser {
public:
    void feed(const char* data, size_t len) { buf_.append(data, len); }
    std::optional<StompFrame> next();

private:
    std::string buf_;
};

std::string escapeHeader(const std::string& s);
std::string unescapeHeader(const std::string& s);

class StompServer {
public:
    explicit StompServer(broker::Broker& broker);
    ~StompServer();
    bool start(const std::string& host, uint16_t port, std::string* error);
    void stop();
    uint16_t port() const;
    broker::Broker& broker() { return broker_; }

private:
    broker::Broker& broker_;
    std::unique_ptr<net::TcpServer> server_;
};

class StompConnection : public std::enable_shared_from_this<StompConnection> {
public:
    StompConnection(StompServer& server, std::shared_ptr<net::TcpConnection> tcp);

    void onData(const char* data, size_t len);
    void onClosed();

    bool hasCredit(const std::string& subscription) const;
    void deliver(const std::string& subscription, broker::QueueBase& queue, broker::Delivery d);
    void subscriptionCancelled(const std::string& subscription, const std::string& reason);

private:
    struct Subscription {
        std::string id;
        std::string destination;
        std::string ackMode = "auto";
        uint16_t prefetch = 0;
        broker::QueuePtr queue;
        std::shared_ptr<broker::Consumer> consumer;
        uint32_t unacked = 0;
    };
    struct Pending {
        std::string subscription;
        broker::QueuePtr queue;
        uint64_t tag = 0;
    };

    void handle(const StompFrame& f);
    void onConnect(const StompFrame& f);
    void onSend(const StompFrame& f);
    void onSubscribe(const StompFrame& f);
    void onUnsubscribe(const StompFrame& f);
    void onAck(const StompFrame& f, bool ack);
    void sendError(const std::string& message, const std::string& detail);
    void sendReceipt(const StompFrame& f);
    broker::QueuePtr resolveQueue(const std::string& destination, bool forSubscribe, std::string* exchange,
                                  std::string* routingKey, std::string* error);
    void cleanup();

    StompServer& server_;
    std::shared_ptr<net::TcpConnection> tcp_;
    StompParser parser_;
    bool connected_ = false;
    bool closed_ = false;
    std::string user_;
    bool tokenAuth_ = false;
    std::shared_ptr<broker::VirtualHost> vhost_;

    mutable std::mutex mu_;
    std::map<std::string, Subscription> subscriptions_;
    std::map<std::string, Pending> pending_;  // ack id -> delivery
    uint64_t nextAckId_ = 1;
};

}  // namespace tq::stomp
