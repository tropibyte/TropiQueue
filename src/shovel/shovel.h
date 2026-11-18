// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Dynamic shovels: move messages from a source (a queue, or an exchange via a
// temporary queue) to a destination (an exchange or a queue), usually on
// another broker. Shovels are runtime parameters of component "shovel":
//
//   PUT /api/parameters/shovel/%2F/orders-to-dc2
//   {"value": {"src-uri": "amqp://user:pass@localhost/%2F", "src-queue": "orders",
//              "dest-uri": "amqp://user:pass@dc2.example/%2F", "dest-exchange": "orders",
//              "ack-mode": "on-confirm"}}
//
// Each shovel runs on its own thread with two client connections and
// reconnects after "reconnect-delay" seconds when either side fails.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include "client/channel.h"
#include "client/connection.h"
#include "common/json.h"

namespace tq::broker {
class Broker;
struct RuntimeParameter;
}  // namespace tq::broker

namespace tq::shovel {

constexpr const char* kComponent = "shovel";

enum class AckMode { OnConfirm, OnPublish, NoAck };
const char* ackModeName(AckMode m);

struct ShovelDef {
    std::string vhost;
    std::string name;

    std::string srcUri;
    std::string srcQueue;
    std::string srcExchange;
    std::string srcExchangeKey = "#";
    uint16_t srcPrefetch = 1000;
    // "never", "queue-length" or a message count.
    std::string srcDeleteAfter = "never";

    std::string destUri;
    std::string destExchange;
    std::optional<std::string> destExchangeKey;
    std::string destQueue;
    bool destAddForwardHeaders = false;
    bool destAddTimestampHeader = false;
    Json destPublishProperties;  // object of property overrides

    AckMode ackMode = AckMode::OnConfirm;
    int reconnectDelaySec = 5;

    static std::optional<ShovelDef> fromJson(const std::string& vhost, const std::string& name, const Json& value,
                                             std::string* error);
    Json toJson() const;
};

enum class ShovelState { Starting, Running, Terminated };
const char* shovelStateName(ShovelState s);

// A running shovel.
class Shovel {
public:
    using CompletionCallback = std::function<void(const ShovelDef&)>;

    Shovel(ShovelDef def, CompletionCallback onComplete);
    ~Shovel();
    Shovel(const Shovel&) = delete;
    Shovel& operator=(const Shovel&) = delete;

    void start();
    void stop();
    void restart();

    const ShovelDef& def() const { return def_; }
    ShovelState state() const { return state_.load(); }
    uint64_t forwarded() const { return forwarded_.load(); }
    Json status() const;

private:
    struct Pending {
        client::Delivery delivery;
    };

    void run();
    bool session();  // one connect/forward cycle; false = do not reconnect
    void setupSource(client::Channel& src, int64_t* remaining);
    void forward(client::Channel& dest, const client::Delivery& d);
    amqp::BasicProperties outgoingProperties(const client::Delivery& d) const;
    bool waitInterruptible(int64_t ms);
    void setError(const std::string& e);

    ShovelDef def_;
    CompletionCallback onComplete_;
    std::atomic<ShovelState> state_{ShovelState::Starting};
    std::atomic<uint64_t> forwarded_{0};
    std::atomic<bool> stopping_{false};
    std::atomic<bool> restartRequested_{false};

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<client::Delivery> inbox_;
    std::string lastError_;
    int64_t stateSinceMs_ = 0;
    std::string sourceQueueName_;
    std::shared_ptr<client::Connection> srcConn_;
    std::shared_ptr<client::Connection> destConn_;
    std::thread thread_;
};

// Owns the shovels of a node and keeps them in sync with the "shovel"
// runtime parameters.
class ShovelManager {
public:
    explicit ShovelManager(broker::Broker& broker);
    ~ShovelManager();

    // Registers the parameter component; called before metadata is loaded.
    static void registerComponent(broker::Broker& broker);
    static bool validate(const std::string& vhost, const std::string& name, const Json& value, std::string* error);

    void start();
    void stop();

    std::vector<Json> status(const std::string& vhost = "") const;
    bool restart(const std::string& vhost, const std::string& name);
    size_t count() const;

private:
    void onParameter(const broker::RuntimeParameter& p, bool removed);
    void launch(const ShovelDef& def);
    void completed(const ShovelDef& def);

    broker::Broker& broker_;
    mutable std::mutex mu_;
    std::map<std::pair<std::string, std::string>, std::unique_ptr<Shovel>> shovels_;
    bool running_ = false;
};

// Removes the user-info part of an AMQP URI ("amqp://user:pass@host" ->
// "amqp://host").
std::string redactUri(const std::string& uri);

}  // namespace tq::shovel
