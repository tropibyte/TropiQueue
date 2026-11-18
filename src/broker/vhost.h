// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

#include "broker/exchange.h"
#include "broker/queue.h"
#include "common/json.h"

namespace tq::broker {

class Broker;

// Result of a topology operation: code 0 on success, otherwise an AMQP
// reply code (404, 403, 405, 406, ...) and a reply text.
struct OpResult {
    uint16_t code = 0;
    std::string text;
    bool ok() const { return code == 0; }
    static OpResult success() { return {}; }
    static OpResult fail(uint16_t c, std::string t) { return OpResult{c, std::move(t)}; }
    Json toJson() const;
    static OpResult fromJson(const Json& j);
};

struct RouteResult {
    std::vector<QueuePtr> queues;
    bool exchangeFound = true;
    bool delayed = false;  // held by a delayed-message exchange
};

struct VhostLimits {
    int64_t maxConnections = -1;
    int64_t maxQueues = -1;
    Json toJson() const;
    static VhostLimits fromJson(const Json& j);
};

// A virtual host: an isolated namespace of exchanges, queues and bindings.
class VirtualHost : public DeadLetterSink, public std::enable_shared_from_this<VirtualHost> {
public:
    VirtualHost(std::string name, Broker& broker);
    ~VirtualHost() override;

    const std::string& name() const { return name_; }
    void createDefaultExchanges();

    // ----- exchanges
    OpResult declareExchange(const ExchangeDef& def, bool passive);
    OpResult deleteExchange(const std::string& name, bool ifUnused);
    std::optional<ExchangeDef> exchangeDef(const std::string& name) const;
    std::vector<ExchangeDef> exchanges() const;

    // ----- queues
    OpResult declareQueue(QueueDef def, bool passive, QueuePtr* out);
    OpResult deleteQueue(const std::string& name, bool ifUnused, bool ifEmpty, uint32_t* messageCount);
    QueuePtr findQueue(const std::string& name) const;
    std::vector<QueuePtr> queues() const;
    size_t queueCount() const;

    // ----- bindings
    OpResult bind(const Binding& b);
    OpResult unbind(const Binding& b);
    std::vector<Binding> bindings() const;

    // ----- routing
    RouteResult route(const std::string& exchange, const MessagePtr& msg);
    // Routes and enqueues; returns the number of queues that accepted it.
    size_t publish(const std::string& exchange, const MessagePtr& msg, bool* routed = nullptr);

    void deadLetter(const QueueBase& from, const MessagePtr& msg, const std::string& reason) override;

    // ----- lifecycle
    void tick(int64_t nowMs);
    void deleteExclusiveQueues(uint64_t connectionId);
    void maybeAutoDelete(const QueuePtr& q);
    void reapplyPolicies();
    void shutdown();

    VhostLimits limits() const;
    void setLimits(const VhostLimits& l);
    bool tracing() const { return tracing_.load(); }
    void setTracing(bool on) { tracing_ = on; }
    std::atomic<int64_t>& connectionCount() { return connections_; }

    // Durable definitions for the metadata snapshot.
    Json durableDefinitions() const;

private:
    void routeExchange(const Exchange& ex, const Message& msg, const std::string& routingKey,
                       std::vector<QueuePtr>& out) const;
    void addUnique(std::vector<QueuePtr>& out, const QueuePtr& q) const;
    void scheduleDelayed(const std::string& exchange, const MessagePtr& msg, int64_t delayMs);
    void removeQueueLocked(const QueuePtr& q);

    std::string name_;
    Broker& broker_;
    mutable std::shared_mutex mu_;
    std::map<std::string, std::unique_ptr<Exchange>> exchanges_;
    std::map<std::string, QueuePtr> queues_;
    VhostLimits limits_;
    std::atomic<int64_t> connections_{0};
    std::atomic<uint64_t> delayedPending_{0};
    std::atomic<bool> tracing_{false};
};

using VhostPtr = std::shared_ptr<VirtualHost>;

}  // namespace tq::broker
