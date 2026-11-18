// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "client/recovery.h"

#include <algorithm>
#include <chrono>

#include "common/logging.h"

namespace tq::client {

// ------------------------------------------------------------- RecoveringChannel

RecoveringChannel::RecoveringChannel(std::weak_ptr<RecoveringConnection> owner, std::shared_ptr<Channel> channel)
    : owner_(std::move(owner)), channel_(std::move(channel)) {}

std::shared_ptr<Channel> RecoveringChannel::current() const {
    std::lock_guard<std::mutex> lock(mu_);
    if (!channel_) throw ClientError(0, "channel is not available (connection is recovering)");
    return channel_;
}

bool RecoveringChannel::isOpen() const {
    std::lock_guard<std::mutex> lock(mu_);
    return channel_ && channel_->isOpen() && !closedByUser_;
}

void RecoveringChannel::declareExchange(const std::string& name, const std::string& type, bool durable,
                                        bool autoDelete, bool internal, const amqp::FieldTable& args) {
    current()->declareExchange(name, type, durable, autoDelete, internal, args);
    if (auto o = owner_.lock()) {
        RecoveringConnection::ExchangeRecord r;
        r.type = type;
        r.durable = durable;
        r.autoDelete = autoDelete;
        r.internal = internal;
        r.arguments = args;
        o->recordExchange(name, std::move(r));
    }
}

void RecoveringChannel::deleteExchange(const std::string& name, bool ifUnused) {
    current()->deleteExchange(name, ifUnused);
    if (auto o = owner_.lock()) o->forgetExchange(name);
}

void RecoveringChannel::bindExchange(const std::string& destination, const std::string& source,
                                     const std::string& routingKey, const amqp::FieldTable& args) {
    current()->bindExchange(destination, source, routingKey, args);
    if (auto o = owner_.lock()) {
        RecoveringConnection::BindingRecord r;
        r.toExchange = true;
        r.destination = destination;
        r.source = source;
        r.routingKey = routingKey;
        r.arguments = args;
        o->recordBinding(std::move(r));
    }
}

QueueInfo RecoveringChannel::declareQueue(const std::string& name, bool durable, bool exclusive, bool autoDelete,
                                          const amqp::FieldTable& args) {
    QueueInfo info = current()->declareQueue(name, durable, exclusive, autoDelete, args);
    if (auto o = owner_.lock()) {
        RecoveringConnection::QueueRecord r;
        r.name = info.name;
        r.durable = durable;
        r.exclusive = exclusive;
        r.autoDelete = autoDelete;
        r.serverNamed = name.empty();
        r.arguments = args;
        o->recordQueue(std::move(r));
    }
    return info;
}

uint32_t RecoveringChannel::deleteQueue(const std::string& name, bool ifUnused, bool ifEmpty) {
    uint32_t n = current()->deleteQueue(name, ifUnused, ifEmpty);
    if (auto o = owner_.lock()) o->forgetQueue(name);
    return n;
}

void RecoveringChannel::bindQueue(const std::string& queue, const std::string& exchange,
                                  const std::string& routingKey, const amqp::FieldTable& args) {
    current()->bindQueue(queue, exchange, routingKey, args);
    if (auto o = owner_.lock()) {
        RecoveringConnection::BindingRecord r;
        r.destination = queue;
        r.source = exchange;
        r.routingKey = routingKey;
        r.arguments = args;
        o->recordBinding(std::move(r));
    }
}

void RecoveringChannel::unbindQueue(const std::string& queue, const std::string& exchange,
                                    const std::string& routingKey, const amqp::FieldTable& args) {
    current()->unbindQueue(queue, exchange, routingKey, args);
    if (auto o = owner_.lock()) {
        RecoveringConnection::BindingRecord r;
        r.destination = queue;
        r.source = exchange;
        r.routingKey = routingKey;
        r.arguments = args;
        o->forgetBinding(r);
    }
}

void RecoveringChannel::qos(uint16_t prefetchCount, bool global) {
    current()->qos(prefetchCount, global);
    std::lock_guard<std::mutex> lock(mu_);
    qos_ = std::make_pair(prefetchCount, global);
}

void RecoveringChannel::publish(const std::string& exchange, const std::string& routingKey, const std::string& body,
                                const amqp::BasicProperties& props, bool mandatory) {
    current()->publish(exchange, routingKey, body, props, mandatory);
}

Channel::ConsumerCallback RecoveringChannel::wrap(Channel::ConsumerCallback cb) {
    std::weak_ptr<RecoveringChannel> weak = weak_from_this();
    return [weak, cb](const Delivery& d) {
        auto self = weak.lock();
        if (!self) return;
        Delivery copy = d;
        {
            std::lock_guard<std::mutex> lock(self->mu_);
            copy.deliveryTag = d.deliveryTag + self->tagOffset_;
            self->highestTagSeen_ = std::max(self->highestTagSeen_, copy.deliveryTag);
        }
        cb(copy);
    };
}

std::string RecoveringChannel::consume(const std::string& queue, Channel::ConsumerCallback cb, bool noAck,
                                       const std::string& tag, bool exclusive, const amqp::FieldTable& args) {
    std::string actual = current()->consume(queue, wrap(cb), noAck, tag, exclusive, args);
    std::lock_guard<std::mutex> lock(mu_);
    ConsumerRecord r;
    r.queue = queue;
    r.callback = std::move(cb);
    r.noAck = noAck;
    r.exclusive = exclusive;
    r.arguments = args;
    consumers_[actual] = std::move(r);
    return actual;
}

void RecoveringChannel::cancel(const std::string& consumerTag) {
    current()->cancel(consumerTag);
    std::lock_guard<std::mutex> lock(mu_);
    consumers_.erase(consumerTag);
}

std::optional<uint64_t> RecoveringChannel::toBrokerTag(uint64_t tag) const {
    std::lock_guard<std::mutex> lock(mu_);
    if (tag <= tagOffset_) return std::nullopt;  // delivered before the last recovery
    return tag - tagOffset_;
}

void RecoveringChannel::ack(uint64_t deliveryTag, bool multiple) {
    if (auto t = toBrokerTag(deliveryTag)) current()->ack(*t, multiple);
}

void RecoveringChannel::nack(uint64_t deliveryTag, bool multiple, bool requeue) {
    if (auto t = toBrokerTag(deliveryTag)) current()->nack(*t, multiple, requeue);
}

void RecoveringChannel::reject(uint64_t deliveryTag, bool requeue) {
    if (auto t = toBrokerTag(deliveryTag)) current()->reject(*t, requeue);
}

void RecoveringChannel::confirmSelect() {
    current()->confirmSelect();
    std::lock_guard<std::mutex> lock(mu_);
    confirmMode_ = true;
}

bool RecoveringChannel::waitForConfirms(int timeoutMs) { return current()->waitForConfirms(timeoutMs); }

void RecoveringChannel::close() {
    std::shared_ptr<Channel> ch;
    {
        std::lock_guard<std::mutex> lock(mu_);
        closedByUser_ = true;
        ch = channel_;
        consumers_.clear();
    }
    if (ch) ch->close();
}

void RecoveringChannel::recoverOn(const std::shared_ptr<Connection>& conn) {
    std::optional<std::pair<uint16_t, bool>> qos;
    bool confirm = false;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (closedByUser_) return;
        qos = qos_;
        confirm = confirmMode_;
    }
    auto ch = conn->openChannel();
    if (qos) ch->qos(qos->first, qos->second);
    if (confirm) ch->confirmSelect();
    std::lock_guard<std::mutex> lock(mu_);
    channel_ = ch;
    tagOffset_ = highestTagSeen_;
}

void RecoveringChannel::recoverConsumers() {
    std::map<std::string, ConsumerRecord> consumers;
    std::shared_ptr<Channel> ch;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (closedByUser_) return;
        consumers = consumers_;
        ch = channel_;
    }
    for (auto& [tag, r] : consumers) {
        try {
            ch->consume(r.queue, wrap(r.callback), r.noAck, tag, r.exclusive, r.arguments);
        } catch (const ClientError& e) {
            LOG_WARN("client") << "cannot recover consumer " << tag << " on queue '" << r.queue << "': " << e.what();
        }
    }
}

// ---------------------------------------------------------- RecoveringConnection

RecoveringConnection::RecoveringConnection(ConnectionParams params, RecoveryOptions options)
    : params_(std::move(params)), options_(options) {}

std::shared_ptr<RecoveringConnection> RecoveringConnection::open(const ConnectionParams& params,
                                                                 RecoveryOptions options, std::string* error) {
    auto conn = Connection::open(params, error);
    if (!conn) return nullptr;
    std::shared_ptr<RecoveringConnection> rc(new RecoveringConnection(params, options));
    rc->conn_ = conn;
    rc->monitor_ = std::thread([raw = rc.get()] { raw->monitor(); });
    return rc;
}

RecoveringConnection::~RecoveringConnection() { close(); }

std::shared_ptr<Connection> RecoveringConnection::current() const {
    std::lock_guard<std::mutex> lock(mu_);
    return conn_;
}

bool RecoveringConnection::isOpen() const {
    auto c = current();
    return c && c->isOpen() && !closing_;
}

void RecoveringConnection::onConnectionLost(std::function<void(const std::string&)> cb) {
    std::lock_guard<std::mutex> lock(mu_);
    onLost_ = std::move(cb);
}

void RecoveringConnection::onRecovered(std::function<void()> cb) {
    std::lock_guard<std::mutex> lock(mu_);
    onRecovered_ = std::move(cb);
}

void RecoveringConnection::onRecoveryFailed(std::function<void(const std::string&)> cb) {
    std::lock_guard<std::mutex> lock(mu_);
    onFailed_ = std::move(cb);
}

std::shared_ptr<RecoveringChannel> RecoveringConnection::openChannel() {
    auto conn = current();
    if (!conn || !conn->isOpen()) throw ClientError(0, "connection is not open");
    auto ch = std::make_shared<RecoveringChannel>(weak_from_this(), conn->openChannel());
    std::lock_guard<std::mutex> lock(mu_);
    channels_.erase(std::remove_if(channels_.begin(), channels_.end(), [](const auto& w) { return w.expired(); }),
                    channels_.end());
    channels_.push_back(ch);
    return ch;
}

void RecoveringConnection::close() {
    if (closing_.exchange(true)) return;
    cv_.notify_all();
    if (monitor_.joinable()) {
        if (monitor_.get_id() == std::this_thread::get_id()) monitor_.detach();
        else monitor_.join();
    }
    auto conn = current();
    if (conn) conn->close();
}

void RecoveringConnection::recordExchange(const std::string& name, ExchangeRecord r) {
    std::lock_guard<std::mutex> lock(mu_);
    exchanges_[name] = std::move(r);
}

void RecoveringConnection::forgetExchange(const std::string& name) {
    std::lock_guard<std::mutex> lock(mu_);
    exchanges_.erase(name);
    bindings_.erase(std::remove_if(bindings_.begin(), bindings_.end(),
                                   [&](const BindingRecord& b) {
                                       return b.source == name || (b.toExchange && b.destination == name);
                                   }),
                    bindings_.end());
}

void RecoveringConnection::recordQueue(QueueRecord r) {
    std::lock_guard<std::mutex> lock(mu_);
    queues_[r.name] = std::move(r);
}

void RecoveringConnection::forgetQueue(const std::string& name) {
    std::lock_guard<std::mutex> lock(mu_);
    queues_.erase(name);
    bindings_.erase(std::remove_if(bindings_.begin(), bindings_.end(),
                                   [&](const BindingRecord& b) { return !b.toExchange && b.destination == name; }),
                    bindings_.end());
}

void RecoveringConnection::recordBinding(BindingRecord r) {
    std::lock_guard<std::mutex> lock(mu_);
    if (std::find(bindings_.begin(), bindings_.end(), r) == bindings_.end()) bindings_.push_back(std::move(r));
}

void RecoveringConnection::forgetBinding(const BindingRecord& r) {
    std::lock_guard<std::mutex> lock(mu_);
    bindings_.erase(std::remove(bindings_.begin(), bindings_.end(), r), bindings_.end());
}

void RecoveringConnection::monitor() {
    while (!closing_) {
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait_for(lock, std::chrono::milliseconds(200), [this] { return closing_.load(); });
        }
        if (closing_) return;
        auto conn = current();
        if (conn && conn->isOpen()) continue;

        std::string reason = conn ? conn->closeReason() : "not connected";
        std::function<void(const std::string&)> lost;
        {
            std::lock_guard<std::mutex> lock(mu_);
            lost = onLost_;
        }
        if (lost) lost(reason);
        LOG_INFO("client") << "connection to " << params_.host << ":" << params_.port << " lost (" << reason
                           << "), recovering";

        recovering_ = true;
        int attempt = 0;
        std::string error;
        bool ok = false;
        while (!closing_) {
            ++attempt;
            if (recover(&error)) {
                ok = true;
                break;
            }
            LOG_WARN("client") << "recovery attempt " << attempt << " failed: " << error;
            if (options_.maxAttempts >= 0 && attempt >= options_.maxAttempts) break;
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait_for(lock, std::chrono::milliseconds(options_.networkRecoveryIntervalMs),
                         [this] { return closing_.load(); });
        }
        recovering_ = false;
        if (closing_) return;
        if (ok) {
            recoveries_++;
            std::function<void()> cb;
            {
                std::lock_guard<std::mutex> lock(mu_);
                cb = onRecovered_;
            }
            if (cb) cb();
        } else {
            std::function<void(const std::string&)> cb;
            {
                std::lock_guard<std::mutex> lock(mu_);
                cb = onFailed_;
            }
            if (cb) cb(error);
            return;
        }
    }
}

bool RecoveringConnection::recover(std::string* error) {
    auto conn = Connection::open(params_, error);
    if (!conn) return false;
    try {
        std::vector<std::shared_ptr<RecoveringChannel>> channels;
        {
            std::lock_guard<std::mutex> lock(mu_);
            conn_ = conn;
            for (const auto& w : channels_) {
                if (auto c = w.lock()) channels.push_back(c);
            }
        }
        if (options_.topologyRecovery) {
            auto ch = conn->openChannel();
            recoverTopology(*ch);
            ch->close();
        }
        for (auto& c : channels) c->recoverOn(conn);
        if (options_.topologyRecovery) {
            for (auto& c : channels) c->recoverConsumers();
        }
        return true;
    } catch (const ClientError& e) {
        if (error) *error = e.what();
        conn->close();
        return false;
    }
}

void RecoveringConnection::recoverTopology(Channel& ch) {
    std::map<std::string, ExchangeRecord> exchanges;
    std::map<std::string, QueueRecord> queues;
    std::vector<BindingRecord> bindings;
    {
        std::lock_guard<std::mutex> lock(mu_);
        exchanges = exchanges_;
        queues = queues_;
        bindings = bindings_;
    }
    for (const auto& [name, r] : exchanges) {
        ch.declareExchange(name, r.type, r.durable, r.autoDelete, r.internal, r.arguments);
    }
    std::map<std::string, QueueRecord> recovered;
    for (const auto& [name, r] : queues) {
        QueueInfo info = ch.declareQueue(r.serverNamed ? "" : name, r.durable, r.exclusive, r.autoDelete, r.arguments);
        QueueRecord copy = r;
        copy.name = info.name;
        recovered[info.name] = copy;
    }
    for (const auto& b : bindings) {
        if (b.toExchange) ch.bindExchange(b.destination, b.source, b.routingKey, b.arguments);
        else ch.bindQueue(b.destination, b.source, b.routingKey, b.arguments);
    }
    std::lock_guard<std::mutex> lock(mu_);
    queues_ = std::move(recovered);
}

}  // namespace tq::client
