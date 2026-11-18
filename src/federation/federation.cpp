// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "federation/federation.h"

#include <algorithm>
#include <chrono>

#include "broker/broker.h"
#include "broker/parameters.h"
#include "broker/vhost.h"
#include "client/uri.h"
#include "common/clock.h"
#include "common/logging.h"
#include "common/strings.h"
#include "shovel/shovel.h"

namespace tq::federation {

using amqp::FieldArray;
using amqp::FieldTable;
using amqp::FieldValue;

// ------------------------------------------------------------------ upstreams

std::optional<Upstream> Upstream::fromJson(const std::string& vhost, const std::string& name, const Json& v,
                                           std::string* error) {
    auto fail = [&](const std::string& why) -> std::optional<Upstream> {
        if (error) *error = why;
        return std::nullopt;
    };
    if (!v.isObject()) return fail("upstream definition must be a JSON object");
    Upstream u;
    u.vhost = vhost;
    u.name = name;
    const Json& uri = v["uri"];
    if (uri.isString()) u.uri = uri.asString();
    else if (uri.isArray() && !uri.asArray().empty()) u.uri = uri.asArray().front().asString();
    if (u.uri.empty()) return fail("uri is required");
    std::string why;
    if (!client::parseUri(u.uri, &why)) return fail("invalid uri: " + why);
    if (v.contains("exchange")) u.exchange = v["exchange"].asString();
    if (v.contains("max-hops")) {
        int64_t hops = v["max-hops"].asInt(0);
        if (hops < 1) return fail("max-hops must be at least 1");
        u.maxHops = static_cast<int>(hops);
    }
    if (v.contains("prefetch-count")) {
        int64_t n = v["prefetch-count"].asInt(0);
        if (n < 1 || n > 65535) return fail("prefetch-count must be between 1 and 65535");
        u.prefetchCount = static_cast<uint16_t>(n);
    }
    if (v.contains("reconnect-delay")) {
        int64_t d = v["reconnect-delay"].asInt(-1);
        if (d < 0) return fail("reconnect-delay must not be negative");
        u.reconnectDelaySec = static_cast<int>(d);
    }
    u.expiresMs = v["expires"].asInt(0);
    u.messageTtlMs = v["message-ttl"].asInt(0);
    if (u.expiresMs < 0 || u.messageTtlMs < 0) return fail("expires and message-ttl must not be negative");
    u.trustUserId = v["trust-user-id"].asBool(false);
    if (v.contains("queue")) return fail("queue federation is not supported; use exchange federation or a shovel");
    return u;
}

// ------------------------------------------------------------ header helpers

namespace {

const FieldArray* receivedFrom(const FieldTable* headers) {
    if (!headers) return nullptr;
    const FieldValue* v = headers->get(kReceivedFromHeader);
    if (!v || !v->array) return nullptr;
    return v->array.get();
}

}  // namespace

size_t receivedFromCount(const FieldTable* headers) {
    const FieldArray* a = receivedFrom(headers);
    return a ? a->size() : 0;
}

bool receivedFromContains(const FieldTable* headers, const std::string& clusterName) {
    const FieldArray* a = receivedFrom(headers);
    if (!a) return false;
    for (const auto& entry : *a) {
        if (!entry.table) continue;
        if (entry.table->getString("cluster-name").value_or("") == clusterName) return true;
    }
    return false;
}

FieldTable appendReceivedFrom(const FieldTable* headers, const std::string& uri, const std::string& exchange,
                              bool redelivered, const std::string& clusterName) {
    FieldTable out = headers ? *headers : FieldTable{};
    FieldArray hops;
    if (const FieldArray* a = receivedFrom(headers)) hops = *a;
    FieldTable entry;
    entry.set("uri", FieldValue::longString(uri));
    entry.set("exchange", FieldValue::longString(exchange));
    entry.set("redelivered", FieldValue::boolean(redelivered));
    entry.set("cluster-name", FieldValue::longString(clusterName));
    hops.push_back(FieldValue::fromTable(std::move(entry)));
    out.set(kReceivedFromHeader, FieldValue::fromArray(std::move(hops)));
    return out;
}

// ----------------------------------------------------------------------- Link

Link::Link(broker::Broker& broker, std::string vhost, std::string exchange, Upstream upstream,
           std::string localClusterName)
    : broker_(broker),
      vhost_(std::move(vhost)),
      exchange_(std::move(exchange)),
      upstream_(std::move(upstream)),
      localClusterName_(std::move(localClusterName)) {}

Link::~Link() { stop(); }

std::string Link::upstreamQueueName() const { return "federation: " + exchange_ + " -> " + localClusterName_; }

void Link::start() {
    stopping_ = false;
    statusSinceMs_ = wallClockMs();
    thread_ = std::thread([this] { run(); });
}

void Link::stop() {
    stopping_ = true;
    cv_.notify_all();
    std::shared_ptr<client::Connection> c;
    {
        std::lock_guard<std::mutex> lock(mu_);
        c = conn_;
    }
    if (c) c->close();
    if (thread_.joinable() && thread_.get_id() != std::this_thread::get_id()) thread_.join();
}

void Link::setStatus(const std::string& status, const std::string& error) {
    std::lock_guard<std::mutex> lock(mu_);
    if (status_ != status) statusSinceMs_ = wallClockMs();
    status_ = status;
    error_ = error;
    if (!error.empty()) {
        LOG_WARN("federation") << "link " << exchange_ << " -> upstream '" << upstream_.name << "' in vhost '"
                               << vhost_ << "': " << error;
    }
}

Json Link::status() const {
    Json j = Json::object();
    j["vhost"] = vhost_;
    j["exchange"] = exchange_;
    j["upstream"] = upstream_.name;
    j["upstream_exchange"] = upstream_.exchange.value_or(exchange_);
    j["uri"] = shovel::redactUri(upstream_.uri);
    j["type"] = "exchange";
    j["messages_received"] = static_cast<int64_t>(received_.load());
    j["messages_dropped"] = static_cast<int64_t>(dropped_.load());
    std::lock_guard<std::mutex> lock(mu_);
    j["status"] = status_;
    j["timestamp"] = statusSinceMs_;
    if (!error_.empty()) j["error"] = error_;
    if (!upstreamClusterName_.empty()) j["upstream_cluster_name"] = upstreamClusterName_;
    return j;
}

bool Link::waitInterruptible(int64_t ms) {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait_for(lock, std::chrono::milliseconds(ms), [this] { return stopping_.load(); });
    return !stopping_;
}

void Link::run() {
    while (!stopping_) {
        bool again = session();
        std::shared_ptr<client::Connection> c;
        {
            std::lock_guard<std::mutex> lock(mu_);
            c.swap(conn_);
            inbox_.clear();
        }
        if (c) c->close();
        if (stopping_ || !again) break;
        if (!waitInterruptible(std::max(1, upstream_.reconnectDelaySec) * 1000)) break;
    }
    setStatus("terminated");
}

std::set<std::string> Link::downstreamBindingKeys() const {
    std::set<std::string> keys;
    auto vh = broker_.vhost(vhost_);
    if (!vh) return keys;
    auto def = vh->exchangeDef(exchange_);
    if (!def) return keys;
    if (def->type == broker::ExchangeType::Fanout) {
        keys.insert("");
        return keys;
    }
    for (const auto& b : vh->bindings()) {
        if (b.source == exchange_) keys.insert(b.routingKey);
    }
    return keys;
}

void Link::syncBindings(client::Channel& ch, std::set<std::string>& bound) {
    std::string upstreamExchange = upstream_.exchange.value_or(exchange_);
    std::set<std::string> wanted = downstreamBindingKeys();
    for (const auto& key : wanted) {
        if (bound.count(key)) continue;
        ch.bindQueue(upstreamQueueName(), upstreamExchange, key);
        bound.insert(key);
    }
    for (auto it = bound.begin(); it != bound.end();) {
        if (wanted.count(*it)) {
            ++it;
            continue;
        }
        ch.unbindQueue(upstreamQueueName(), upstreamExchange, *it);
        it = bound.erase(it);
    }
}

bool Link::session() {
    std::string err;
    auto params = client::parseUri(upstream_.uri, &err);
    if (!params) {
        setStatus("error", "invalid upstream URI: " + err);
        return false;
    }
    params->connectionName = "Federation link (" + vhost_ + "/" + exchange_ + ")";
    try {
        setStatus("starting");
        auto conn = client::Connection::open(*params, &err);
        if (!conn) {
            setStatus("error", "cannot connect to upstream: " + err);
            return true;
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            conn_ = conn;
            upstreamClusterName_ = conn->serverProperties().getString("cluster_name").value_or(params->host);
        }
        auto ch = conn->openChannel();
        FieldTable args;
        if (upstream_.expiresMs > 0) args.set("x-expires", FieldValue::int64(upstream_.expiresMs));
        if (upstream_.messageTtlMs > 0) args.set("x-message-ttl", FieldValue::int64(upstream_.messageTtlMs));
        ch->declareQueue(upstreamQueueName(), true, false, false, args);
        std::set<std::string> bound;
        syncBindings(*ch, bound);
        ch->qos(upstream_.prefetchCount);
        ch->consume(upstreamQueueName(), [this](const client::Delivery& d) {
            {
                std::lock_guard<std::mutex> lock(mu_);
                inbox_.push_back(d);
            }
            cv_.notify_one();
        });
        setStatus("running");

        int64_t lastSync = monotonicMs();
        while (!stopping_) {
            std::deque<client::Delivery> batch;
            {
                std::unique_lock<std::mutex> lock(mu_);
                cv_.wait_for(lock, std::chrono::milliseconds(250),
                             [this] { return !inbox_.empty() || stopping_.load(); });
                batch.swap(inbox_);
            }
            if (!conn->isOpen()) {
                setStatus("error", "upstream connection closed: " + conn->closeReason());
                return true;
            }
            for (const auto& d : batch) {
                received_++;
                if (!republish(d)) dropped_++;
                ch->ack(d.deliveryTag);
            }
            if (monotonicMs() - lastSync >= 2000) {
                syncBindings(*ch, bound);
                lastSync = monotonicMs();
            }
        }
        return true;
    } catch (const client::ClientError& e) {
        setStatus("error", e.what());
    } catch (const std::exception& e) {
        setStatus("error", e.what());
    }
    return true;
}

bool Link::republish(const client::Delivery& d) {
    const FieldTable* headers = d.properties.headers ? &*d.properties.headers : nullptr;
    if (receivedFromCount(headers) >= static_cast<size_t>(upstream_.maxHops)) return false;
    std::string upstreamName;
    {
        std::lock_guard<std::mutex> lock(mu_);
        upstreamName = upstreamClusterName_;
    }
    if (receivedFromContains(headers, upstreamName)) return false;

    auto vh = broker_.vhost(vhost_);
    if (!vh) return false;
    auto msg = std::make_shared<broker::Message>();
    msg->id = broker::nextMessageId();
    msg->exchange = exchange_;
    msg->routingKey = d.routingKey;
    msg->props = d.properties;
    msg->props.headers = appendReceivedFrom(headers, shovel::redactUri(upstream_.uri),
                                            upstream_.exchange.value_or(exchange_), d.redelivered, upstreamName);
    if (!upstream_.trustUserId) msg->props.userId.reset();
    msg->body = d.body;
    msg->publishedAtMs = wallClockMs();
    msg->publisherVhost = vhost_;
    vh->publish(exchange_, msg);
    return true;
}

// --------------------------------------------------------- FederationManager

FederationManager::FederationManager(broker::Broker& broker) : broker_(broker) {}

FederationManager::~FederationManager() { stop(); }

void FederationManager::registerComponents(broker::Broker& broker) {
    broker.parameters().registerComponent(
        kUpstreamComponent, [](const std::string& vhost, const std::string& name, const Json& value, std::string* error) {
            return Upstream::fromJson(vhost, name, value, error).has_value();
        });
    broker.parameters().registerComponent(
        kUpstreamSetComponent, [](const std::string&, const std::string&, const Json& value, std::string* error) {
            if (!value.isArray()) {
                if (error) *error = "an upstream set must be a JSON array";
                return false;
            }
            for (const auto& e : value.asArray()) {
                if (!e.isObject() || e["upstream"].asString().empty()) {
                    if (error) *error = "each upstream set entry needs an \"upstream\" name";
                    return false;
                }
            }
            return true;
        });
}

void FederationManager::start(int64_t reconcileIntervalMs) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (running_) return;
        running_ = true;
    }
    task_ = broker_.scheduler().scheduleEvery(reconcileIntervalMs, [this] { reconcile(); });
}

void FederationManager::stop() {
    std::map<LinkKey, std::unique_ptr<Link>> all;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_ && links_.empty()) return;
        running_ = false;
        all.swap(links_);
        linkFingerprints_.clear();
    }
    if (task_) broker_.scheduler().cancel(task_);
    task_ = 0;
    std::lock_guard<std::mutex> rl(reconcileMu_);
    for (auto& [k, l] : all) l->stop();
}

std::vector<Upstream> FederationManager::upstreamsFor(const std::string& vhost, const Json& def) const {
    std::vector<Upstream> out;
    auto& params = broker_.parameters();
    auto load = [&](const std::string& name) {
        auto p = params.get(kUpstreamComponent, vhost, name);
        if (!p) return;
        std::string err;
        if (auto u = Upstream::fromJson(vhost, name, p->value, &err)) out.push_back(*u);
    };
    if (def.contains("federation-upstream")) {
        load(def["federation-upstream"].asString());
        return out;
    }
    std::string set = def["federation-upstream-set"].asString();
    if (set.empty()) return out;
    if (set == "all") {
        for (const auto& p : params.list(kUpstreamComponent, vhost)) load(p.name);
        return out;
    }
    auto sp = params.get(kUpstreamSetComponent, vhost, set);
    if (!sp) return out;
    for (const auto& e : sp->value.asArray()) {
        std::string name = e["upstream"].asString();
        auto p = params.get(kUpstreamComponent, vhost, name);
        if (!p) continue;
        Json merged = p->value;
        for (const auto& [k, v] : e.asObject()) {
            if (k != "upstream") merged[k] = v;
        }
        std::string err;
        if (auto u = Upstream::fromJson(vhost, name, merged, &err)) out.push_back(*u);
    }
    return out;
}

void FederationManager::reconcile() {
    std::lock_guard<std::mutex> rl(reconcileMu_);
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) return;
    }
    std::string clusterName = broker_.parameters().clusterName(broker_.nodeName());
    std::map<LinkKey, Upstream> wanted;
    std::map<LinkKey, std::string> fingerprints;
    for (const auto& vh : broker_.vhosts()) {
        for (const auto& ex : vh->exchanges()) {
            if (ex.name.empty() || ex.internal || str::startsWith(ex.name, "amq.")) continue;
            auto policy = broker_.policies().matchExchange(vh->name(), ex.name);
            if (!policy) continue;
            for (auto& u : upstreamsFor(vh->name(), policy->definition)) {
                LinkKey key{vh->name(), ex.name, u.name};
                Json fp = Json::object();
                fp["uri"] = u.uri;
                fp["exchange"] = u.exchange.value_or("");
                fp["max-hops"] = static_cast<int64_t>(u.maxHops);
                fp["prefetch"] = static_cast<int64_t>(u.prefetchCount);
                fp["expires"] = u.expiresMs;
                fp["ttl"] = u.messageTtlMs;
                fp["trust"] = u.trustUserId;
                fingerprints[key] = fp.dump() + "|" + clusterName;
                wanted.emplace(key, std::move(u));
            }
        }
    }

    std::vector<std::unique_ptr<Link>> stale;
    std::vector<std::pair<LinkKey, Upstream>> toStart;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto it = links_.begin(); it != links_.end();) {
            auto w = wanted.find(it->first);
            if (w == wanted.end() || linkFingerprints_[it->first] != fingerprints[it->first]) {
                stale.push_back(std::move(it->second));
                linkFingerprints_.erase(it->first);
                it = links_.erase(it);
            } else {
                ++it;
            }
        }
        for (auto& [key, u] : wanted) {
            if (!links_.count(key)) toStart.emplace_back(key, u);
        }
    }
    for (auto& l : stale) l->stop();
    for (auto& [key, u] : toStart) {
        auto link = std::make_unique<Link>(broker_, std::get<0>(key), std::get<1>(key), u, clusterName);
        link->start();
        LOG_INFO("federation") << "started link " << std::get<1>(key) << " <- upstream '" << u.name
                               << "' in vhost '" << std::get<0>(key) << "'";
        std::lock_guard<std::mutex> lock(mu_);
        linkFingerprints_[key] = fingerprints[key];
        links_[key] = std::move(link);
    }
}

std::vector<Json> FederationManager::status(const std::string& vhost) const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<Json> out;
    for (const auto& [key, link] : links_) {
        if (!vhost.empty() && std::get<0>(key) != vhost) continue;
        out.push_back(link->status());
    }
    return out;
}

size_t FederationManager::linkCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return links_.size();
}

bool FederationManager::restartLink(const std::string& vhost, const std::string& exchange,
                                    const std::string& upstream) {
    std::unique_ptr<Link> link;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = links_.find({vhost, exchange, upstream});
        if (it == links_.end()) return false;
        link = std::move(it->second);
        links_.erase(it);
        linkFingerprints_.erase({vhost, exchange, upstream});
    }
    link->stop();
    reconcile();
    return true;
}

}  // namespace tq::federation
