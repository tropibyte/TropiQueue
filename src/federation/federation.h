// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Exchange federation. A federated exchange receives copies of the messages
// published to an exchange of the same name (or the upstream's "exchange")
// on one or more upstream brokers, without the brokers being clustered.
//
// Upstreams are runtime parameters:
//
//   component "federation-upstream", value:
//     {"uri": "amqp://user:pass@upstream-host/%2F", "max-hops": 1, "prefetch-count": 1000,
//      "reconnect-delay": 5, "expires": 3600000, "message-ttl": 600000, "trust-user-id": false}
//
//   component "federation-upstream-set", value: [{"upstream": "dc1"}, {"upstream": "dc2"}]
//
// and are attached to exchanges with a policy key: "federation-upstream-set"
// ("all" or the name of a set) or "federation-upstream" (one upstream).
//
// For every (exchange, upstream) pair a link connects to the upstream,
// declares a queue "federation: <exchange> -> <cluster name>", binds it to
// the upstream exchange with the binding keys used downstream, and
// republishes what it receives into the local exchange. Each hop appends an
// entry to the "x-received-from" header, which bounds propagation (max-hops)
// and prevents cycles.
#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "amqp/field_table.h"
#include "client/channel.h"
#include "client/connection.h"
#include "common/executor.h"
#include "common/json.h"

namespace tq::broker {
class Broker;
struct RuntimeParameter;
}  // namespace tq::broker

namespace tq::federation {

constexpr const char* kUpstreamComponent = "federation-upstream";
constexpr const char* kUpstreamSetComponent = "federation-upstream-set";
constexpr const char* kReceivedFromHeader = "x-received-from";

struct Upstream {
    std::string vhost;
    std::string name;
    std::string uri;
    std::optional<std::string> exchange;  // defaults to the downstream exchange name
    int maxHops = 1;
    uint16_t prefetchCount = 1000;
    int reconnectDelaySec = 5;
    int64_t expiresMs = 0;     // x-expires of the upstream queue, 0 = none
    int64_t messageTtlMs = 0;  // x-message-ttl of the upstream queue, 0 = none
    bool trustUserId = false;

    static std::optional<Upstream> fromJson(const std::string& vhost, const std::string& name, const Json& value,
                                            std::string* error);
};

// Pure helpers, exposed for tests.
// Number of hops recorded in the x-received-from header.
size_t receivedFromCount(const amqp::FieldTable* headers);
// True when cluster `name` already appears in the x-received-from header.
bool receivedFromContains(const amqp::FieldTable* headers, const std::string& clusterName);
// Returns headers with a new x-received-from entry appended.
amqp::FieldTable appendReceivedFrom(const amqp::FieldTable* headers, const std::string& uri,
                                    const std::string& exchange, bool redelivered, const std::string& clusterName);

// One (exchange, upstream) link.
class Link {
public:
    Link(broker::Broker& broker, std::string vhost, std::string exchange, Upstream upstream,
         std::string localClusterName);
    ~Link();
    Link(const Link&) = delete;
    Link& operator=(const Link&) = delete;

    void start();
    void stop();

    const std::string& vhost() const { return vhost_; }
    const std::string& exchange() const { return exchange_; }
    const Upstream& upstream() const { return upstream_; }
    Json status() const;
    uint64_t received() const { return received_.load(); }
    uint64_t dropped() const { return dropped_.load(); }

    // Name of the queue declared on the upstream broker.
    std::string upstreamQueueName() const;

private:
    void run();
    bool session();
    void syncBindings(client::Channel& ch, std::set<std::string>& bound);
    std::set<std::string> downstreamBindingKeys() const;
    // Returns false when the message was dropped by loop detection.
    bool republish(const client::Delivery& d);
    bool waitInterruptible(int64_t ms);
    void setStatus(const std::string& status, const std::string& error = "");

    broker::Broker& broker_;
    std::string vhost_;
    std::string exchange_;
    Upstream upstream_;
    std::string localClusterName_;
    std::string upstreamClusterName_;

    std::atomic<bool> stopping_{false};
    std::atomic<uint64_t> received_{0};
    std::atomic<uint64_t> dropped_{0};

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<client::Delivery> inbox_;
    std::string status_ = "starting";
    std::string error_;
    int64_t statusSinceMs_ = 0;
    std::shared_ptr<client::Connection> conn_;
    std::thread thread_;
};

// Keeps the set of running links in sync with upstream parameters, policies
// and exchanges.
class FederationManager {
public:
    explicit FederationManager(broker::Broker& broker);
    ~FederationManager();

    static void registerComponents(broker::Broker& broker);

    void start(int64_t reconcileIntervalMs = 2000);
    void stop();

    // Recomputes which links should exist and starts/stops them.
    void reconcile();

    std::vector<Json> status(const std::string& vhost = "") const;
    size_t linkCount() const;
    bool restartLink(const std::string& vhost, const std::string& exchange, const std::string& upstream);

private:
    using LinkKey = std::tuple<std::string, std::string, std::string>;  // vhost, exchange, upstream

    std::vector<Upstream> upstreamsFor(const std::string& vhost, const Json& policyDefinition) const;

    broker::Broker& broker_;
    mutable std::mutex mu_;
    std::map<LinkKey, std::unique_ptr<Link>> links_;
    std::map<LinkKey, std::string> linkFingerprints_;
    Scheduler::TaskId task_ = 0;
    std::mutex reconcileMu_;
    bool running_ = false;
};

}  // namespace tq::federation
