// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "mqtt/mqtt_server.h"

#include "broker/broker.h"
#include "broker/vhost.h"
#include "common/clock.h"
#include "common/logging.h"
#include "common/random.h"
#include "common/strings.h"
#include "security/users.h"

namespace tq::mqtt {

using security::Access;

namespace {

class MqttHandler : public net::ConnectionHandler {
public:
    explicit MqttHandler(std::shared_ptr<MqttConnection> c) : conn_(std::move(c)) {}
    void onData(const char* data, size_t len) override { conn_->onData(data, len); }
    void onClosed() override { conn_->onClosed(); }
    void onTick(int64_t nowMs) override { conn_->onTick(nowMs); }

private:
    std::shared_ptr<MqttConnection> conn_;
};

// Bridges a subscription queue to the MQTT connection.
class SubscriptionConsumer : public broker::Consumer {
public:
    SubscriptionConsumer(std::weak_ptr<MqttConnection> conn, std::string tag, uint8_t qos, uint64_t connectionId)
        : conn_(std::move(conn)), tag_(std::move(tag)), qos_(qos), connectionId_(connectionId) {}
    const std::string& tag() const override { return tag_; }
    bool noAck() const override { return qos_ == 0; }
    int priority() const override { return 0; }
    uint64_t connectionId() const override { return connectionId_; }
    bool exclusive() const override { return false; }
    const amqp::FieldTable& arguments() const override { return args_; }
    bool hasCredit() const override {
        auto c = conn_.lock();
        return c && (qos_ == 0 || c->hasCredit());
    }
    void deliver(broker::QueueBase& queue, broker::Delivery d) override {
        if (auto c = conn_.lock()) c->deliver(queue, std::move(d), qos_);
    }
    void cancelledByServer(const std::string& reason) override {
        if (auto c = conn_.lock()) c->kick(reason);
    }

private:
    std::weak_ptr<MqttConnection> conn_;
    std::string tag_;
    uint8_t qos_;
    uint64_t connectionId_;
    amqp::FieldTable args_;
};

}  // namespace

// ----------------------------------------------------------------- MqttServer

MqttServer::MqttServer(broker::Broker& broker, MqttOptions options) : broker_(broker), options_(std::move(options)) {}

MqttServer::~MqttServer() { stop(); }

bool MqttServer::start(const std::string& host, uint16_t port, std::string* error) {
    net::TcpServerOptions opts;
    opts.ioThreads = 1;
    server_ = std::make_unique<net::TcpServer>(
        "mqtt",
        [this](const std::shared_ptr<net::TcpConnection>& tcp) -> std::unique_ptr<net::ConnectionHandler> {
            return std::make_unique<MqttHandler>(std::make_shared<MqttConnection>(*this, tcp));
        },
        opts);
    return server_->start(host, port, error);
}

void MqttServer::stop() {
    if (server_) server_->stop();
}

uint16_t MqttServer::port() const { return server_ ? server_->port() : 0; }

std::shared_ptr<MqttConnection> MqttServer::registerClient(const std::string& clientId,
                                                           const std::shared_ptr<MqttConnection>& c) {
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<MqttConnection> previous;
    auto it = clients_.find(clientId);
    if (it != clients_.end()) previous = it->second.lock();
    clients_[clientId] = c;
    return previous;
}

void MqttServer::unregisterClient(const std::string& clientId, const MqttConnection* c) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = clients_.find(clientId);
    if (it == clients_.end()) return;
    auto current = it->second.lock();
    if (!current || current.get() == c) clients_.erase(it);
}

size_t MqttServer::clientCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return clients_.size();
}

void MqttServer::storeRetained(const std::string& topic, const std::string& payload, uint8_t qos) {
    if (!options_.retainedMessages) return;
    std::lock_guard<std::mutex> lock(mu_);
    if (payload.empty()) {
        retained_.erase(topic);
        return;
    }
    retained_[topic] = RetainedMessage{topic, payload, qos};
}

std::vector<RetainedMessage> MqttServer::matchRetained(const std::string& filter) const {
    std::vector<RetainedMessage> out;
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& [topic, msg] : retained_) {
        if (mqttTopicMatches(filter, topic)) out.push_back(msg);
    }
    return out;
}

// ------------------------------------------------------------- MqttConnection

MqttConnection::MqttConnection(MqttServer& server, std::shared_ptr<net::TcpConnection> tcp)
    : server_(server), tcp_(std::move(tcp)) {
    lastReceiveMs_.store(wallClockMs());
}

MqttConnection::~MqttConnection() = default;

void MqttConnection::send(const std::string& bytes) {
    if (tcp_) tcp_->send(bytes);
}

void MqttConnection::kick(const std::string& reason) {
    LOG_INFO("mqtt") << "disconnecting client '" << clientId_ << "': " << reason;
    closed_ = true;
    if (tcp_) tcp_->close();
}

void MqttConnection::onData(const char* data, size_t len) {
    if (closed_) return;
    lastReceiveMs_.store(wallClockMs());
    parser_.feed(data, len);
    try {
        while (!closed_) {
            auto p = parser_.next();
            if (!p) break;
            handle(*p);
        }
    } catch (const DecodeError& e) {
        LOG_WARN("mqtt") << tcp_->name() << ": malformed packet: " << e.what();
        closed_ = true;
        tcp_->close();
    }
}

void MqttConnection::handle(const Packet& p) {
    if (!connected_ && p.type != PacketType::Connect) {
        closed_ = true;
        tcp_->close();
        return;
    }
    switch (p.type) {
        case PacketType::Connect: onConnect(p); break;
        case PacketType::Publish: onPublish(p); break;
        case PacketType::Puback: onPuback(p); break;
        case PacketType::Pubrel: {
            ByteReader r(p.body);
            uint16_t id = r.u16();
            incomingQos2_.erase(id);
            send(encodeAck(PacketType::Pubcomp, id));
            break;
        }
        case PacketType::Subscribe: onSubscribe(p); break;
        case PacketType::Unsubscribe: onUnsubscribe(p); break;
        case PacketType::Pingreq: send(encodePacket(PacketType::Pingresp, 0, "")); break;
        case PacketType::Disconnect:
            will_.hasWill = false;
            cleanup(true);
            tcp_->closeAfterFlush();
            break;
        default: break;
    }
}

void MqttConnection::onConnect(const Packet& p) {
    if (connected_) {
        kick("second CONNECT");
        return;
    }
    ConnectPacket c = ConnectPacket::decode(p.body);
    if (c.protocolName != "MQTT" && c.protocolName != "MQIsdp") {
        send(encodeConnack(false, kConnBadProtocol));
        tcp_->closeAfterFlush();
        closed_ = true;
        return;
    }
    auto& broker = server_.broker();
    std::string vhost = server_.options().vhost;
    std::string user;
    std::string password;
    if (c.username) {
        user = *c.username;
        size_t colon = user.find(':');
        if (colon != std::string::npos) {
            vhost = user.substr(0, colon);
            user = user.substr(colon + 1);
        }
        password = c.password.value_or("");
    } else if (server_.options().allowAnonymous) {
        user = server_.options().defaultUser;
        password = server_.options().defaultPass;
    } else {
        send(encodeConnack(false, kConnNotAuthorized));
        tcp_->closeAfterFlush();
        closed_ = true;
        return;
    }
    auto auth = broker.authenticator().authenticatePassword(user, password, tcp_->peerAddress());
    if (!auth.ok) {
        send(encodeConnack(false, kConnBadCredentials));
        tcp_->closeAfterFlush();
        closed_ = true;
        return;
    }
    vhost_ = broker.vhost(vhost);
    if (!vhost_ || !broker.users().canAccessVhost(auth.user, vhost)) {
        if (auth.token) broker.users().dropTokenSession(auth.user);
        send(encodeConnack(false, kConnNotAuthorized));
        tcp_->closeAfterFlush();
        closed_ = true;
        return;
    }
    user_ = auth.user;
    tokenAuth_ = auth.token;
    cleanSession_ = c.cleanSession;
    keepAlive_ = c.keepAlive;
    clientId_ = c.clientId;
    if (clientId_.empty()) {
        if (!cleanSession_) {
            send(encodeConnack(false, kConnIdentifierRejected));
            tcp_->closeAfterFlush();
            closed_ = true;
            return;
        }
        clientId_ = rnd::uniqueName("tq-mqtt-", 16);
    }
    if (c.hasWill) will_ = c;

    auto previous = server_.registerClient(clientId_, shared_from_this());
    if (previous && previous.get() != this) previous->kick("client id taken over by a new connection");

    bool sessionPresent = false;
    if (!cleanSession_) {
        for (uint8_t qos : {uint8_t{0}, uint8_t{1}}) {
            if (vhost_->findQueue(queueName(qos))) {
                sessionPresent = true;
                std::string err;
                ensureSubscriptionQueue(qos, &err);
            }
        }
    } else {
        for (uint8_t qos : {uint8_t{0}, uint8_t{1}}) {
            if (auto q = vhost_->findQueue(queueName(qos))) {
                Json cmd = Json::object();
                cmd["op"] = "delete_queue";
                cmd["vhost"] = vhost_->name();
                cmd["name"] = q->name();
                if (q->durable()) broker.commitMetadata(cmd);
                else vhost_->deleteQueue(q->name(), false, false, nullptr);
            }
        }
    }
    connected_ = true;
    send(encodeConnack(sessionPresent, kConnAccepted));
    LOG_INFO("mqtt") << "accepted MQTT connection " << tcp_->name() << " (client id '" << clientId_ << "', user '"
                     << user_ << "')";
}

std::string MqttConnection::queueName(uint8_t qos) const {
    return "mqtt-subscription-" + clientId_ + "qos" + std::to_string(qos);
}

bool MqttConnection::ensureSubscriptionQueue(uint8_t qos, std::string* error) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (queues_.count(qos)) return true;
    }
    broker::QueueDef def;
    def.name = queueName(qos);
    def.vhost = vhost_->name();
    def.durable = !cleanSession_ && qos > 0;
    def.autoDelete = cleanSession_;
    broker::QueuePtr q = vhost_->findQueue(def.name);
    if (!q) {
        if (def.durable) {
            Json cmd = Json::object();
            cmd["op"] = "declare_queue";
            cmd["def"] = def.toJson();
            server_.broker().commitMetadata(cmd);
            q = vhost_->findQueue(def.name);
        } else {
            vhost_->declareQueue(def, false, &q);
        }
    }
    if (!q) {
        if (error) *error = "cannot declare subscription queue";
        return false;
    }
    auto consumer = std::make_shared<SubscriptionConsumer>(weak_from_this(), "mqtt-" + clientId_, qos,
                                                           tcp_ ? tcp_->id() : 0);
    {
        std::lock_guard<std::mutex> lock(mu_);
        queues_[qos] = q;
        consumers_[qos] = consumer;
    }
    return q->addConsumer(consumer, error);
}

void MqttConnection::onPublish(const Packet& p) {
    PublishPacket pub = PublishPacket::decode(p.flags, p.body);
    auto& broker = server_.broker();
    const auto& opts = server_.options();
    if (!isValidTopicName(pub.topic)) {
        kick("invalid topic name");
        return;
    }
    std::string rk = topicToRoutingKey(pub.topic);
    if (!broker.users().checkResource(user_, vhost_->name(), Access::Write, opts.exchange) ||
        !broker.users().checkTopic(user_, vhost_->name(), opts.exchange, rk, true)) {
        kick("not authorised to publish to '" + pub.topic + "'");
        return;
    }
    if (pub.retain) server_.storeRetained(pub.topic, pub.payload, pub.qos);
    amqp::BasicProperties props;
    props.deliveryMode = pub.qos > 0 ? 2 : 1;
    props.mutableHeaders().set("x-mqtt-publish-qos", amqp::FieldValue::uint8(pub.qos));
    auto msg = broker::makeMessage(opts.exchange, rk, props, pub.payload);
    vhost_->publish(opts.exchange, msg);
    if (pub.qos == 1) {
        send(encodeAck(PacketType::Puback, pub.packetId));
    } else if (pub.qos == 2) {
        incomingQos2_.insert(pub.packetId);
        send(encodeAck(PacketType::Pubrec, pub.packetId));
    }
}

void MqttConnection::onSubscribe(const Packet& p) {
    SubscribePacket s = SubscribePacket::decode(p.body);
    auto& broker = server_.broker();
    const auto& opts = server_.options();
    std::vector<uint8_t> codes;
    std::vector<std::string> granted;
    for (auto& [filter, requested] : s.filters) {
        uint8_t qos = std::min<uint8_t>(requested, 1);
        std::string err;
        if (!isValidTopicFilter(filter) ||
            !broker.users().checkResource(user_, vhost_->name(), Access::Read, opts.exchange) ||
            !ensureSubscriptionQueue(qos, &err)) {
            codes.push_back(0x80);
            continue;
        }
        broker::Binding b;
        b.source = opts.exchange;
        b.destination = queueName(qos);
        b.routingKey = filterToBindingKey(filter);
        broker::QueuePtr q;
        {
            std::lock_guard<std::mutex> lock(mu_);
            q = queues_[qos];
        }
        if (q && q->durable()) {
            Json cmd = Json::object();
            cmd["op"] = "bind";
            cmd["vhost"] = vhost_->name();
            cmd["binding"] = b.toJson(vhost_->name());
            broker.commitMetadata(cmd);
        } else {
            vhost_->bind(b);
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            subscriptions_[filter] = qos;
        }
        codes.push_back(qos);
        granted.push_back(filter);
    }
    send(encodeSuback(s.packetId, codes));
    for (const auto& filter : granted) {
        for (const auto& r : server_.matchRetained(filter)) {
            PublishPacket out;
            out.topic = r.topic;
            out.payload = r.payload;
            out.retain = true;
            out.qos = 0;
            send(out.encode());
        }
    }
}

void MqttConnection::onUnsubscribe(const Packet& p) {
    UnsubscribePacket u = UnsubscribePacket::decode(p.body);
    for (const auto& filter : u.filters) {
        uint8_t qos = 0;
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = subscriptions_.find(filter);
            if (it == subscriptions_.end()) continue;
            qos = it->second;
            subscriptions_.erase(it);
        }
        broker::Binding b;
        b.source = server_.options().exchange;
        b.destination = queueName(qos);
        b.routingKey = filterToBindingKey(filter);
        vhost_->unbind(b);
    }
    send(encodeAck(PacketType::Unsuback, u.packetId));
}

void MqttConnection::onPuback(const Packet& p) {
    ByteReader r(p.body);
    uint16_t id = r.u16();
    std::pair<broker::QueueBase*, uint64_t> entry{nullptr, 0};
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = inflight_.find(id);
        if (it == inflight_.end()) return;
        entry = it->second;
        inflight_.erase(it);
    }
    if (entry.first) entry.first->settle({entry.second}, broker::Settlement::Ack);
}

bool MqttConnection::hasCredit() const {
    std::lock_guard<std::mutex> lock(mu_);
    return inflight_.size() < server_.options().maxInflight;
}

void MqttConnection::deliver(broker::QueueBase& queue, broker::Delivery d, uint8_t maxQos) {
    PublishPacket out;
    out.topic = routingKeyToTopic(d.msg->routingKey);
    out.payload = d.msg->body;
    uint8_t msgQos = d.msg->persistent() ? 1 : 0;
    if (d.msg->props.headers) {
        if (auto q = d.msg->props.headers->getInt("x-mqtt-publish-qos")) msgQos = static_cast<uint8_t>(*q);
    }
    out.qos = std::min<uint8_t>(maxQos, std::min<uint8_t>(msgQos, 1));
    out.dup = d.redelivered;
    if (out.qos == 1) {
        std::lock_guard<std::mutex> lock(mu_);
        out.packetId = nextPacketId_++;
        if (nextPacketId_ == 0) nextPacketId_ = 1;
        inflight_[out.packetId] = {&queue, d.queueTag};
    } else if (maxQos == 1) {
        queue.settle({d.queueTag}, broker::Settlement::Ack);
    }
    send(out.encode());
}

void MqttConnection::publishWill() {
    if (!will_.hasWill || !vhost_) return;
    amqp::BasicProperties props;
    props.deliveryMode = will_.willQos > 0 ? 2 : 1;
    const std::string& ex = server_.options().exchange;
    vhost_->publish(ex, broker::makeMessage(ex, topicToRoutingKey(will_.willTopic), props, will_.willMessage));
    if (will_.willRetain) server_.storeRetained(will_.willTopic, will_.willMessage, will_.willQos);
    will_.hasWill = false;
}

void MqttConnection::cleanup(bool graceful) {
    if (!connected_) return;
    connected_ = false;
    if (tokenAuth_) server_.broker().users().dropTokenSession(user_);
    if (!graceful) publishWill();
    std::map<uint8_t, broker::QueuePtr> queues;
    std::map<uint16_t, std::pair<broker::QueueBase*, uint64_t>> inflight;
    {
        std::lock_guard<std::mutex> lock(mu_);
        queues.swap(queues_);
        inflight.swap(inflight_);
        consumers_.clear();
    }
    for (auto& [id, e] : inflight) e.first->settle({e.second}, broker::Settlement::Requeue);
    for (auto& [qos, q] : queues) {
        q->removeConsumer("mqtt-" + clientId_, tcp_ ? tcp_->id() : 0);
        if (vhost_) vhost_->maybeAutoDelete(q);
    }
    server_.unregisterClient(clientId_, this);
}

void MqttConnection::onClosed() {
    closed_ = true;
    cleanup(false);
}

void MqttConnection::onTick(int64_t nowMs) {
    if (!connected_ || keepAlive_ == 0) return;
    if (nowMs - lastReceiveMs_.load() > static_cast<int64_t>(keepAlive_) * 1500) {
        LOG_INFO("mqtt") << "client '" << clientId_ << "' missed keep-alive, disconnecting";
        closed_ = true;
        tcp_->close();
    }
}

}  // namespace tq::mqtt
