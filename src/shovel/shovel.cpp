// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "shovel/shovel.h"

#include <chrono>

#include "broker/broker.h"
#include "broker/parameters.h"
#include "client/uri.h"
#include "common/clock.h"
#include "common/logging.h"
#include "common/strings.h"

namespace tq::shovel {

using amqp::FieldArray;
using amqp::FieldTable;
using amqp::FieldValue;

const char* ackModeName(AckMode m) {
    switch (m) {
        case AckMode::OnConfirm: return "on-confirm";
        case AckMode::OnPublish: return "on-publish";
        case AckMode::NoAck: return "no-ack";
    }
    return "on-confirm";
}

const char* shovelStateName(ShovelState s) {
    switch (s) {
        case ShovelState::Starting: return "starting";
        case ShovelState::Running: return "running";
        case ShovelState::Terminated: return "terminated";
    }
    return "terminated";
}

std::string redactUri(const std::string& uri) {
    size_t scheme = uri.find("://");
    if (scheme == std::string::npos) return uri;
    size_t authorityStart = scheme + 3;
    size_t authorityEnd = uri.find_first_of("/?", authorityStart);
    if (authorityEnd == std::string::npos) authorityEnd = uri.size();
    size_t at = uri.rfind('@', authorityEnd);
    if (at == std::string::npos || at < authorityStart) return uri;
    return uri.substr(0, authorityStart) + uri.substr(at + 1);
}

// ---------------------------------------------------------------- definition

namespace {

std::optional<std::string> firstUri(const Json& v) {
    if (v.isString()) return v.asString();
    if (v.isArray() && !v.asArray().empty() && v.asArray().front().isString()) return v.asArray().front().asString();
    return std::nullopt;
}

bool validUri(const std::string& uri, std::string* error) {
    std::string why;
    if (!client::parseUri(uri, &why)) {
        if (error) *error = "invalid URI '" + redactUri(uri) + "': " + why;
        return false;
    }
    return true;
}

}  // namespace

std::optional<ShovelDef> ShovelDef::fromJson(const std::string& vhost, const std::string& name, const Json& v,
                                             std::string* error) {
    auto fail = [&](const std::string& why) -> std::optional<ShovelDef> {
        if (error) *error = why;
        return std::nullopt;
    };
    if (!v.isObject()) return fail("shovel definition must be a JSON object");
    ShovelDef d;
    d.vhost = vhost;
    d.name = name;

    auto src = firstUri(v["src-uri"]);
    if (!src) return fail("src-uri is required");
    auto dest = firstUri(v["dest-uri"]);
    if (!dest) return fail("dest-uri is required");
    if (!validUri(*src, error) || !validUri(*dest, error)) return std::nullopt;
    d.srcUri = *src;
    d.destUri = *dest;

    d.srcQueue = v["src-queue"].asString();
    d.srcExchange = v["src-exchange"].asString();
    if (v.contains("src-exchange-key")) d.srcExchangeKey = v["src-exchange-key"].asString();
    if (d.srcQueue.empty() == !v.contains("src-exchange")) {
        return fail("exactly one of src-queue and src-exchange must be given");
    }
    if (v.contains("src-prefetch-count")) {
        int64_t n = v["src-prefetch-count"].asInt(-1);
        if (n < 1 || n > 65535) return fail("src-prefetch-count must be between 1 and 65535");
        d.srcPrefetch = static_cast<uint16_t>(n);
    }
    const Json& del = v["src-delete-after"];
    if (del.isString()) {
        d.srcDeleteAfter = del.asString();
        if (d.srcDeleteAfter != "never" && d.srcDeleteAfter != "queue-length") {
            return fail("src-delete-after must be \"never\", \"queue-length\" or a number");
        }
    } else if (del.isNumber()) {
        if (del.asInt(-1) < 0) return fail("src-delete-after must not be negative");
        d.srcDeleteAfter = std::to_string(del.asInt());
    } else if (!del.isNull()) {
        return fail("src-delete-after must be \"never\", \"queue-length\" or a number");
    }
    if (d.srcDeleteAfter == "queue-length" && d.srcQueue.empty()) {
        return fail("src-delete-after \"queue-length\" requires src-queue");
    }

    d.destExchange = v["dest-exchange"].asString();
    if (v.contains("dest-exchange-key")) d.destExchangeKey = v["dest-exchange-key"].asString();
    d.destQueue = v["dest-queue"].asString();
    if (!d.destQueue.empty() && v.contains("dest-exchange")) {
        return fail("dest-queue and dest-exchange are mutually exclusive");
    }
    d.destAddForwardHeaders = v["dest-add-forward-headers"].asBool(false);
    d.destAddTimestampHeader = v["dest-add-timestamp-header"].asBool(false);
    d.destPublishProperties = v["dest-publish-properties"];
    if (!d.destPublishProperties.isNull() && !d.destPublishProperties.isObject()) {
        return fail("dest-publish-properties must be an object");
    }

    std::string ack = v["ack-mode"].asString("on-confirm");
    if (ack == "on-confirm") d.ackMode = AckMode::OnConfirm;
    else if (ack == "on-publish") d.ackMode = AckMode::OnPublish;
    else if (ack == "no-ack") d.ackMode = AckMode::NoAck;
    else return fail("ack-mode must be one of on-confirm, on-publish, no-ack");

    if (v.contains("reconnect-delay")) {
        int64_t delay = v["reconnect-delay"].asInt(-1);
        if (delay < 0) return fail("reconnect-delay must not be negative");
        d.reconnectDelaySec = static_cast<int>(delay);
    }
    return d;
}

Json ShovelDef::toJson() const {
    Json j = Json::object();
    j["src-uri"] = srcUri;
    if (!srcQueue.empty()) j["src-queue"] = srcQueue;
    else {
        j["src-exchange"] = srcExchange;
        j["src-exchange-key"] = srcExchangeKey;
    }
    j["src-prefetch-count"] = static_cast<int64_t>(srcPrefetch);
    if (auto n = str::parseInt(srcDeleteAfter)) j["src-delete-after"] = *n;
    else j["src-delete-after"] = srcDeleteAfter;
    j["dest-uri"] = destUri;
    if (!destQueue.empty()) j["dest-queue"] = destQueue;
    if (!destExchange.empty()) j["dest-exchange"] = destExchange;
    if (destExchangeKey) j["dest-exchange-key"] = *destExchangeKey;
    j["dest-add-forward-headers"] = destAddForwardHeaders;
    j["dest-add-timestamp-header"] = destAddTimestampHeader;
    if (destPublishProperties.isObject()) j["dest-publish-properties"] = destPublishProperties;
    j["ack-mode"] = ackModeName(ackMode);
    j["reconnect-delay"] = static_cast<int64_t>(reconnectDelaySec);
    return j;
}

// ---------------------------------------------------------------------- Shovel

Shovel::Shovel(ShovelDef def, CompletionCallback onComplete)
    : def_(std::move(def)), onComplete_(std::move(onComplete)) {}

Shovel::~Shovel() { stop(); }

void Shovel::start() {
    stopping_ = false;
    stateSinceMs_ = wallClockMs();
    thread_ = std::thread([this] { run(); });
}

void Shovel::stop() {
    stopping_ = true;
    cv_.notify_all();
    std::shared_ptr<client::Connection> src, dest;
    {
        std::lock_guard<std::mutex> lock(mu_);
        src = srcConn_;
        dest = destConn_;
    }
    if (src) src->close();
    if (dest) dest->close();
    if (thread_.joinable() && thread_.get_id() != std::this_thread::get_id()) thread_.join();
}

void Shovel::restart() {
    restartRequested_ = true;
    cv_.notify_all();
}

void Shovel::setError(const std::string& e) {
    std::lock_guard<std::mutex> lock(mu_);
    lastError_ = e;
    LOG_WARN("shovel") << "shovel '" << def_.name << "' in vhost '" << def_.vhost << "': " << e;
}

bool Shovel::waitInterruptible(int64_t ms) {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait_for(lock, std::chrono::milliseconds(ms), [this] { return stopping_.load() || restartRequested_.load(); });
    return !stopping_;
}

Json Shovel::status() const {
    Json j = Json::object();
    j["name"] = def_.name;
    j["vhost"] = def_.vhost;
    j["type"] = "dynamic";
    j["state"] = shovelStateName(state_.load());
    j["src_uri"] = def_.srcUri;
    j["dest_uri"] = def_.destUri;
    if (!def_.srcQueue.empty()) j["src_queue"] = def_.srcQueue;
    if (!def_.srcExchange.empty() || def_.srcQueue.empty()) {
        j["src_exchange"] = def_.srcExchange;
        j["src_exchange_key"] = def_.srcExchangeKey;
    }
    if (!def_.destQueue.empty()) j["dest_queue"] = def_.destQueue;
    if (!def_.destExchange.empty()) j["dest_exchange"] = def_.destExchange;
    j["ack_mode"] = ackModeName(def_.ackMode);
    j["messages_forwarded"] = static_cast<int64_t>(forwarded_.load());
    std::lock_guard<std::mutex> lock(mu_);
    j["timestamp"] = stateSinceMs_;
    if (!lastError_.empty()) j["last_error"] = lastError_;
    return j;
}

void Shovel::run() {
    LOG_INFO("shovel") << "starting shovel '" << def_.name << "' in vhost '" << def_.vhost << "'";
    while (!stopping_) {
        restartRequested_ = false;
        state_ = ShovelState::Starting;
        bool again = session();
        {
            std::shared_ptr<client::Connection> src, dest;
            {
                std::lock_guard<std::mutex> lock(mu_);
                src.swap(srcConn_);
                dest.swap(destConn_);
                inbox_.clear();
            }
            if (src) src->close();
            if (dest) dest->close();
        }
        if (stopping_ || !again) break;
        if (restartRequested_) continue;
        if (def_.reconnectDelaySec <= 0) break;
        if (!waitInterruptible(static_cast<int64_t>(def_.reconnectDelaySec) * 1000)) break;
    }
    state_ = ShovelState::Terminated;
    std::lock_guard<std::mutex> lock(mu_);
    stateSinceMs_ = wallClockMs();
}

void Shovel::setupSource(client::Channel& src, int64_t* remaining) {
    *remaining = -1;
    if (!def_.srcQueue.empty()) {
        client::QueueInfo info = src.declareQueue(def_.srcQueue, true);
        sourceQueueName_ = info.name;
        if (def_.srcDeleteAfter == "queue-length") *remaining = info.messageCount;
    } else {
        client::QueueInfo info = src.declareQueue("", false, true, true);
        sourceQueueName_ = info.name;
        src.bindQueue(info.name, def_.srcExchange, def_.srcExchangeKey);
    }
    if (def_.srcDeleteAfter != "never" && def_.srcDeleteAfter != "queue-length") {
        *remaining = str::parseInt(def_.srcDeleteAfter).value_or(-1);
    }
}

bool Shovel::session() {
    std::string err;
    auto srcParams = client::parseUri(def_.srcUri, &err);
    auto destParams = client::parseUri(def_.destUri, &err);
    if (!srcParams || !destParams) {
        setError("invalid URI: " + err);
        return false;
    }
    srcParams->connectionName = "Shovel " + def_.name;
    destParams->connectionName = "Shovel " + def_.name;
    try {
        auto src = client::Connection::open(*srcParams, &err);
        if (!src) {
            setError("cannot connect to source: " + err);
            return true;
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            srcConn_ = src;
        }
        auto dest = client::Connection::open(*destParams, &err);
        if (!dest) {
            setError("cannot connect to destination: " + err);
            return true;
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            destConn_ = dest;
        }
        auto srcCh = src->openChannel();
        auto destCh = dest->openChannel();
        if (def_.ackMode == AckMode::OnConfirm) destCh->confirmSelect();
        if (!def_.destQueue.empty()) destCh->declareQueue(def_.destQueue, true);

        int64_t remaining = -1;
        setupSource(*srcCh, &remaining);
        if (remaining == 0) {
            if (onComplete_) onComplete_(def_);
            return false;
        }
        srcCh->qos(def_.srcPrefetch);
        bool noAck = def_.ackMode == AckMode::NoAck;
        srcCh->consume(
            sourceQueueName_,
            [this](const client::Delivery& d) {
                {
                    std::lock_guard<std::mutex> lock(mu_);
                    inbox_.push_back(d);
                }
                cv_.notify_one();
            },
            noAck);

        state_ = ShovelState::Running;
        {
            std::lock_guard<std::mutex> lock(mu_);
            stateSinceMs_ = wallClockMs();
            lastError_.clear();
        }

        while (!stopping_ && !restartRequested_) {
            std::deque<client::Delivery> batch;
            {
                std::unique_lock<std::mutex> lock(mu_);
                cv_.wait_for(lock, std::chrono::milliseconds(200),
                             [this] { return !inbox_.empty() || stopping_.load() || restartRequested_.load(); });
                batch.swap(inbox_);
            }
            if (!src->isOpen()) {
                setError("source connection closed: " + src->closeReason());
                return true;
            }
            if (!dest->isOpen()) {
                setError("destination connection closed: " + dest->closeReason());
                return true;
            }
            uint64_t lastTag = 0;
            for (const auto& d : batch) {
                if (remaining == 0) break;
                forward(*destCh, d);
                forwarded_++;
                if (remaining > 0) --remaining;
                if (def_.ackMode == AckMode::OnPublish) srcCh->ack(d.deliveryTag);
                else if (def_.ackMode == AckMode::OnConfirm) lastTag = d.deliveryTag;
            }
            if (lastTag != 0) {
                destCh->waitForConfirms(10000);
                srcCh->ack(lastTag, true);
            }
            if (remaining == 0) {
                LOG_INFO("shovel") << "shovel '" << def_.name << "' transferred all requested messages";
                if (onComplete_) onComplete_(def_);
                return false;
            }
        }
        return true;
    } catch (const client::ClientError& e) {
        setError(e.what());
    } catch (const std::exception& e) {
        setError(e.what());
    }
    return true;
}

amqp::BasicProperties Shovel::outgoingProperties(const client::Delivery& d) const {
    amqp::BasicProperties props = d.properties;
    const Json& o = def_.destPublishProperties;
    if (o.isObject()) {
        if (o.contains("delivery_mode")) props.deliveryMode = static_cast<uint8_t>(o["delivery_mode"].asInt(1));
        if (o.contains("priority")) props.priority = static_cast<uint8_t>(o["priority"].asInt(0));
        if (o.contains("content_type")) props.contentType = o["content_type"].asString();
        if (o.contains("content_encoding")) props.contentEncoding = o["content_encoding"].asString();
        if (o.contains("expiration")) props.expiration = o["expiration"].asString();
        if (o.contains("app_id")) props.appId = o["app_id"].asString();
        if (o.contains("type")) props.type = o["type"].asString();
        if (o["headers"].isObject()) {
            FieldTable extra = FieldTable::fromJson(o["headers"]);
            for (const auto& [k, v] : extra.entries()) props.mutableHeaders().set(k, v);
        }
    }
    if (def_.destAddForwardHeaders) {
        FieldTable info;
        info.set("shovel-type", FieldValue::longString("dynamic"));
        info.set("shovel-name", FieldValue::longString(def_.name));
        info.set("shovel-vhost", FieldValue::longString(def_.vhost));
        info.set("src-uri", FieldValue::longString(redactUri(def_.srcUri)));
        info.set("dest-uri", FieldValue::longString(redactUri(def_.destUri)));
        if (!def_.srcQueue.empty()) info.set("src-queue", FieldValue::longString(def_.srcQueue));
        else info.set("src-exchange", FieldValue::longString(def_.srcExchange));
        if (!def_.destQueue.empty()) info.set("dest-queue", FieldValue::longString(def_.destQueue));
        else info.set("dest-exchange", FieldValue::longString(def_.destExchange));
        FieldArray hops;
        if (props.headers) {
            if (const auto* existing = props.headers->get("x-shovelled"); existing && existing->array) {
                hops = *existing->array;
            }
        }
        hops.push_back(FieldValue::fromTable(std::move(info)));
        props.mutableHeaders().set("x-shovelled", FieldValue::fromArray(std::move(hops)));
    }
    if (def_.destAddTimestampHeader) {
        props.mutableHeaders().set("x-shovelled-timestamp",
                                   FieldValue::int64(static_cast<int64_t>(wallClockMs() / 1000)));
    }
    return props;
}

void Shovel::forward(client::Channel& dest, const client::Delivery& d) {
    std::string exchange = def_.destExchange;
    std::string routingKey = d.routingKey;
    if (!def_.destQueue.empty()) {
        exchange.clear();
        routingKey = def_.destQueue;
    } else if (def_.destExchange.empty()) {
        exchange = d.exchange;
    }
    if (def_.destExchangeKey) routingKey = *def_.destExchangeKey;
    dest.publish(exchange, routingKey, d.body, outgoingProperties(d));
}

// --------------------------------------------------------------- ShovelManager

ShovelManager::ShovelManager(broker::Broker& broker) : broker_(broker) {}

ShovelManager::~ShovelManager() { stop(); }

void ShovelManager::registerComponent(broker::Broker& broker) {
    broker.parameters().registerComponent(kComponent, &ShovelManager::validate);
}

bool ShovelManager::validate(const std::string& vhost, const std::string& name, const Json& value,
                             std::string* error) {
    return ShovelDef::fromJson(vhost, name, value, error).has_value();
}

void ShovelManager::start() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (running_) return;
        running_ = true;
    }
    // Parameter changes arrive while the metadata lock is held; starting and
    // stopping shovels can block on the broker, so it happens on the
    // scheduler thread instead.
    broker_.parameters().addListener(kComponent, [this](const broker::RuntimeParameter& p, bool removed) {
        broker_.scheduler().scheduleAfter(0, [this, p, removed] { onParameter(p, removed); });
    });
    for (const auto& p : broker_.parameters().list(kComponent)) onParameter(p, false);
}

void ShovelManager::stop() {
    std::map<std::pair<std::string, std::string>, std::unique_ptr<Shovel>> all;
    {
        std::lock_guard<std::mutex> lock(mu_);
        running_ = false;
        all.swap(shovels_);
    }
    for (auto& [key, s] : all) s->stop();
}

void ShovelManager::onParameter(const broker::RuntimeParameter& p, bool removed) {
    std::unique_ptr<Shovel> old;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) return;
        auto it = shovels_.find({p.vhost, p.name});
        if (it != shovels_.end()) {
            old = std::move(it->second);
            shovels_.erase(it);
        }
    }
    if (old) old->stop();
    if (removed) {
        LOG_INFO("shovel") << "shovel '" << p.name << "' in vhost '" << p.vhost << "' deleted";
        return;
    }
    std::string error;
    auto def = ShovelDef::fromJson(p.vhost, p.name, p.value, &error);
    if (!def) {
        LOG_WARN("shovel") << "ignoring invalid shovel '" << p.name << "': " << error;
        return;
    }
    launch(*def);
}

void ShovelManager::launch(const ShovelDef& def) {
    auto s = std::make_unique<Shovel>(def, [this](const ShovelDef& d) { completed(d); });
    s->start();
    std::lock_guard<std::mutex> lock(mu_);
    shovels_[{def.vhost, def.name}] = std::move(s);
}

void ShovelManager::completed(const ShovelDef& def) {
    // Runs on the shovel's own thread: remove the parameter asynchronously so
    // the manager can stop and join the shovel.
    broker::Broker* broker = &broker_;
    std::string vhost = def.vhost, name = def.name;
    broker_.scheduler().scheduleAfter(0, [broker, vhost, name] {
        Json cmd = Json::object();
        cmd["op"] = "clear_parameter";
        cmd["component"] = kComponent;
        cmd["vhost"] = vhost;
        cmd["name"] = name;
        broker->commitMetadata(cmd);
    });
}

std::vector<Json> ShovelManager::status(const std::string& vhost) const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<Json> out;
    for (const auto& [key, s] : shovels_) {
        if (!vhost.empty() && key.first != vhost) continue;
        out.push_back(s->status());
    }
    return out;
}

bool ShovelManager::restart(const std::string& vhost, const std::string& name) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = shovels_.find({vhost, name});
    if (it == shovels_.end()) return false;
    it->second->restart();
    return true;
}

size_t ShovelManager::count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return shovels_.size();
}

}  // namespace tq::shovel
