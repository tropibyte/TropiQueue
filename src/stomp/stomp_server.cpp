// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "stomp/stomp_server.h"

#include "broker/broker.h"
#include "broker/vhost.h"
#include "common/logging.h"
#include "common/platform.h"
#include "common/random.h"
#include "common/strings.h"
#include "security/users.h"

namespace tq::stomp {

using security::Access;

// ---------------------------------------------------------------- frames

std::optional<std::string> StompFrame::header(const std::string& name) const {
    for (const auto& [k, v] : headers) {
        if (k == name) return v;  // first occurrence wins (STOMP 1.2)
    }
    return std::nullopt;
}

void StompFrame::set(const std::string& name, const std::string& value) {
    for (auto& [k, v] : headers) {
        if (k == name) {
            v = value;
            return;
        }
    }
    headers.emplace_back(name, value);
}

std::string escapeHeader(const std::string& s) {
    std::string out;
    for (char c : s) {
        switch (c) {
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case ':': out += "\\c"; break;
            default: out.push_back(c);
        }
    }
    return out;
}

std::string unescapeHeader(const std::string& s) {
    std::string out;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '\\' && i + 1 < s.size()) {
            char n = s[++i];
            if (n == 'n') out.push_back('\n');
            else if (n == 'r') out.push_back('\r');
            else if (n == 'c') out.push_back(':');
            else out.push_back(n);
        } else {
            out.push_back(s[i]);
        }
    }
    return out;
}

std::string StompFrame::encode() const {
    std::string out = command + "\n";
    for (const auto& [k, v] : headers) out += k + ":" + v + "\n";
    out += "\n";
    out += body;
    out.push_back('\0');
    return out;
}

std::optional<StompFrame> StompParser::next() {
    // Skip heart-beat EOLs between frames.
    size_t start = 0;
    while (start < buf_.size() && (buf_[start] == '\n' || buf_[start] == '\r')) ++start;
    if (start > 0) buf_.erase(0, start);
    size_t headerEnd = buf_.find("\n\n");
    size_t crlfEnd = buf_.find("\r\n\r\n");
    size_t sepLen = 2;
    if (crlfEnd != std::string::npos && (headerEnd == std::string::npos || crlfEnd < headerEnd)) {
        headerEnd = crlfEnd;
        sepLen = 4;
    }
    if (headerEnd == std::string::npos) return std::nullopt;
    StompFrame f;
    auto lines = str::split(buf_.substr(0, headerEnd), '\n');
    f.command = str::trim(lines[0]);
    for (size_t i = 1; i < lines.size(); ++i) {
        std::string line = lines[i];
        if (!line.empty() && line.back() == '\r') line.pop_back();
        size_t colon = line.find(':');
        if (colon == std::string::npos) continue;
        f.headers.emplace_back(unescapeHeader(line.substr(0, colon)), unescapeHeader(line.substr(colon + 1)));
    }
    size_t bodyStart = headerEnd + sepLen;
    if (auto cl = f.header("content-length")) {
        auto len = str::parseUint(*cl);
        if (!len) throw DecodeError("invalid content-length");
        if (buf_.size() < bodyStart + *len + 1) return std::nullopt;
        f.body = buf_.substr(bodyStart, *len);
        buf_.erase(0, bodyStart + *len + 1);
        return f;
    }
    size_t nul = buf_.find('\0', bodyStart);
    if (nul == std::string::npos) return std::nullopt;
    f.body = buf_.substr(bodyStart, nul - bodyStart);
    buf_.erase(0, nul + 1);
    return f;
}

// -------------------------------------------------------------- consumer

namespace {

class StompHandler : public net::ConnectionHandler {
public:
    explicit StompHandler(std::shared_ptr<StompConnection> c) : conn_(std::move(c)) {}
    void onData(const char* data, size_t len) override { conn_->onData(data, len); }
    void onClosed() override { conn_->onClosed(); }

private:
    std::shared_ptr<StompConnection> conn_;
};

class StompConsumer : public broker::Consumer {
public:
    StompConsumer(std::weak_ptr<StompConnection> conn, std::string subscription, bool noAck, uint16_t prefetch,
                  uint64_t connectionId)
        : conn_(std::move(conn)),
          tag_("stomp-" + subscription),
          subscription_(std::move(subscription)),
          noAck_(noAck),
          prefetch_(prefetch),
          connectionId_(connectionId) {}
    const std::string& tag() const override { return tag_; }
    bool noAck() const override { return noAck_; }
    int priority() const override { return 0; }
    uint64_t connectionId() const override { return connectionId_; }
    bool exclusive() const override { return false; }
    const amqp::FieldTable& arguments() const override { return args_; }
    uint16_t prefetchCount() const override { return prefetch_; }
    bool hasCredit() const override {
        auto c = conn_.lock();
        return c && c->hasCredit(subscription_);
    }
    void deliver(broker::QueueBase& queue, broker::Delivery d) override {
        if (auto c = conn_.lock()) c->deliver(subscription_, queue, std::move(d));
    }
    void cancelledByServer(const std::string& reason) override {
        if (auto c = conn_.lock()) c->subscriptionCancelled(subscription_, reason);
    }

private:
    std::weak_ptr<StompConnection> conn_;
    std::string tag_;
    std::string subscription_;
    bool noAck_;
    uint16_t prefetch_;
    uint64_t connectionId_;
    amqp::FieldTable args_;
};

}  // namespace

// ------------------------------------------------------------------ server

StompServer::StompServer(broker::Broker& broker) : broker_(broker) {}

StompServer::~StompServer() { stop(); }

bool StompServer::start(const std::string& host, uint16_t port, std::string* error) {
    net::TcpServerOptions opts;
    opts.ioThreads = 1;
    server_ = std::make_unique<net::TcpServer>(
        "stomp",
        [this](const std::shared_ptr<net::TcpConnection>& tcp) -> std::unique_ptr<net::ConnectionHandler> {
            return std::make_unique<StompHandler>(std::make_shared<StompConnection>(*this, tcp));
        },
        opts);
    return server_->start(host, port, error);
}

void StompServer::stop() {
    if (server_) server_->stop();
}

uint16_t StompServer::port() const { return server_ ? server_->port() : 0; }

// -------------------------------------------------------------- connection

StompConnection::StompConnection(StompServer& server, std::shared_ptr<net::TcpConnection> tcp)
    : server_(server), tcp_(std::move(tcp)) {}

void StompConnection::onData(const char* data, size_t len) {
    if (closed_) return;
    parser_.feed(data, len);
    try {
        while (!closed_) {
            auto f = parser_.next();
            if (!f) break;
            handle(*f);
        }
    } catch (const DecodeError& e) {
        sendError("malformed frame", e.what());
    }
}

void StompConnection::sendError(const std::string& message, const std::string& detail) {
    StompFrame err;
    err.command = "ERROR";
    err.set("message", escapeHeader(message));
    err.set("content-type", "text/plain");
    err.body = detail;
    err.set("content-length", std::to_string(err.body.size()));
    tcp_->send(err.encode());
    closed_ = true;
    tcp_->closeAfterFlush();
}

void StompConnection::sendReceipt(const StompFrame& f) {
    auto receipt = f.header("receipt");
    if (!receipt) return;
    StompFrame r;
    r.command = "RECEIPT";
    r.set("receipt-id", escapeHeader(*receipt));
    tcp_->send(r.encode());
}

void StompConnection::handle(const StompFrame& f) {
    if (!connected_ && f.command != "CONNECT" && f.command != "STOMP") {
        sendError("not connected", "send CONNECT first");
        return;
    }
    if (f.command == "CONNECT" || f.command == "STOMP") onConnect(f);
    else if (f.command == "SEND") onSend(f);
    else if (f.command == "SUBSCRIBE") onSubscribe(f);
    else if (f.command == "UNSUBSCRIBE") onUnsubscribe(f);
    else if (f.command == "ACK") onAck(f, true);
    else if (f.command == "NACK") onAck(f, false);
    else if (f.command == "BEGIN" || f.command == "COMMIT" || f.command == "ABORT") sendReceipt(f);
    else if (f.command == "DISCONNECT") {
        sendReceipt(f);
        cleanup();
        closed_ = true;
        tcp_->closeAfterFlush();
    } else {
        sendError("unknown command", "unsupported STOMP command '" + f.command + "'");
    }
}

void StompConnection::onConnect(const StompFrame& f) {
    auto& broker = server_.broker();
    std::string login = f.header("login").value_or(broker.config().defaultUser);
    std::string passcode = f.header("passcode").value_or(f.header("login") ? "" : broker.config().defaultPass);
    std::string host = f.header("host").value_or(broker.config().defaultVhost);
    auto auth = broker.authenticator().authenticatePassword(login, passcode, tcp_->peerAddress());
    if (!auth.ok) {
        sendError("access refused", auth.error);
        return;
    }
    vhost_ = broker.vhost(host);
    if (!vhost_) vhost_ = broker.vhost(broker.config().defaultVhost);
    if (!vhost_ || !broker.users().canAccessVhost(auth.user, vhost_->name())) {
        if (auth.token) broker.users().dropTokenSession(auth.user);
        sendError("access refused", "virtual host '" + host + "' access denied");
        return;
    }
    user_ = auth.user;
    tokenAuth_ = auth.token;
    connected_ = true;
    StompFrame c;
    c.command = "CONNECTED";
    c.set("version", "1.2");
    c.set("server", std::string(kProductName) + "/" + kProductVersion);
    c.set("heart-beat", "0,0");
    c.set("session", rnd::uniqueName("session-", 16));
    tcp_->send(c.encode());
}

broker::QueuePtr StompConnection::resolveQueue(const std::string& destination, bool forSubscribe,
                                                std::string* exchange, std::string* routingKey, std::string* error) {
    auto& users = server_.broker().users();
    const std::string& vh = vhost_->name();
    auto declare = [&](broker::QueueDef def) -> broker::QueuePtr {
        broker::QueuePtr q = vhost_->findQueue(def.name);
        if (q) return q;
        if (def.durable) {
            Json cmd = Json::object();
            cmd["op"] = "declare_queue";
            cmd["def"] = def.toJson();
            server_.broker().commitMetadata(cmd);
            return vhost_->findQueue(def.name);
        }
        vhost_->declareQueue(def, false, &q);
        return q;
    };
    if (str::startsWith(destination, "/queue/")) {
        std::string name = destination.substr(7);
        *exchange = "";
        *routingKey = name;
        broker::QueueDef def;
        def.name = name;
        def.vhost = vh;
        def.durable = true;
        return declare(def);
    }
    if (str::startsWith(destination, "/amq/queue/")) {
        std::string name = destination.substr(11);
        *exchange = "";
        *routingKey = name;
        auto q = vhost_->findQueue(name);
        if (!q && error) *error = "queue '" + name + "' does not exist";
        return q;
    }
    std::string ex, key;
    if (str::startsWith(destination, "/topic/")) {
        ex = "amq.topic";
        key = destination.substr(7);
    } else if (str::startsWith(destination, "/exchange/")) {
        std::string rest = destination.substr(10);
        size_t slash = rest.find('/');
        ex = rest.substr(0, slash);
        key = slash == std::string::npos ? "" : rest.substr(slash + 1);
    } else {
        if (error) *error = "unknown destination '" + destination + "'";
        return nullptr;
    }
    *exchange = ex;
    *routingKey = key;
    if (!forSubscribe) return nullptr;
    if (!users.checkResource(user_, vh, Access::Read, ex)) {
        if (error) *error = "read access to exchange '" + ex + "' refused";
        return nullptr;
    }
    broker::QueueDef def;
    def.name = rnd::uniqueName("stomp-subscription-");
    def.vhost = vh;
    def.durable = false;
    def.exclusive = true;
    def.autoDelete = true;
    def.exclusiveOwner = tcp_->id();
    auto q = declare(def);
    if (q) {
        broker::Binding b;
        b.source = ex;
        b.destination = q->name();
        b.routingKey = key;
        auto r = vhost_->bind(b);
        if (!r.ok()) {
            if (error) *error = r.text;
            return nullptr;
        }
    }
    return q;
}

void StompConnection::onSend(const StompFrame& f) {
    auto dest = f.header("destination");
    if (!dest) {
        sendError("missing destination", "SEND requires a destination header");
        return;
    }
    std::string exchange, routingKey, error;
    resolveQueue(*dest, false, &exchange, &routingKey, &error);
    if (!error.empty()) {
        sendError("invalid destination", error);
        return;
    }
    auto& users = server_.broker().users();
    std::string resource = exchange.empty() ? "amq.default" : exchange;
    if (!users.checkResource(user_, vhost_->name(), Access::Write, resource)) {
        sendError("access refused", "write access to '" + resource + "' refused");
        return;
    }
    amqp::BasicProperties props;
    for (const auto& [k, v] : f.headers) {
        if (k == "destination" || k == "receipt" || k == "content-length" || k == "transaction") continue;
        if (k == "content-type") props.contentType = v;
        else if (k == "correlation-id") props.correlationId = v;
        else if (k == "reply-to") props.replyTo = v;
        else if (k == "persistent") props.deliveryMode = v == "true" ? 2 : 1;
        else if (k == "priority") props.priority = static_cast<uint8_t>(str::parseInt(v).value_or(0));
        else if (k == "expiration") props.expiration = v;
        else if (k == "message-id") props.messageId = v;
        else props.mutableHeaders().set(k, amqp::FieldValue::longString(v));
    }
    auto msg = broker::makeMessage(exchange, routingKey, props, f.body);
    vhost_->publish(exchange, msg);
    sendReceipt(f);
}

void StompConnection::onSubscribe(const StompFrame& f) {
    auto id = f.header("id").value_or("default");
    auto dest = f.header("destination");
    if (!dest) {
        sendError("missing destination", "SUBSCRIBE requires a destination header");
        return;
    }
    std::string exchange, routingKey, error;
    auto q = resolveQueue(*dest, true, &exchange, &routingKey, &error);
    if (!q) {
        sendError("invalid destination", error.empty() ? "cannot subscribe to " + *dest : error);
        return;
    }
    if (!server_.broker().users().checkResource(user_, vhost_->name(), Access::Read, q->name())) {
        sendError("access refused", "read access to queue '" + q->name() + "' refused");
        return;
    }
    Subscription s;
    s.id = id;
    s.destination = *dest;
    s.ackMode = f.header("ack").value_or("auto");
    s.prefetch = static_cast<uint16_t>(str::parseInt(f.header("prefetch-count").value_or("0")).value_or(0));
    s.queue = q;
    s.consumer = std::make_shared<StompConsumer>(weak_from_this(), id, s.ackMode == "auto", s.prefetch, tcp_->id());
    auto consumer = s.consumer;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (subscriptions_.count(id)) {
            sendError("duplicate subscription", "subscription id '" + id + "' already in use");
            return;
        }
        subscriptions_[id] = s;
    }
    std::string err;
    if (!q->addConsumer(consumer, &err)) {
        std::lock_guard<std::mutex> lock(mu_);
        subscriptions_.erase(id);
        sendError("subscribe failed", err);
        return;
    }
    sendReceipt(f);
}

void StompConnection::onUnsubscribe(const StompFrame& f) {
    auto id = f.header("id").value_or("default");
    Subscription s;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = subscriptions_.find(id);
        if (it == subscriptions_.end()) return;
        s = it->second;
        subscriptions_.erase(it);
    }
    s.queue->removeConsumer(s.consumer->tag(), tcp_->id());
    vhost_->maybeAutoDelete(s.queue);
    sendReceipt(f);
}

void StompConnection::onAck(const StompFrame& f, bool ack) {
    auto id = f.header("id").value_or(f.header("message-id").value_or(""));
    Pending p;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = pending_.find(id);
        if (it == pending_.end()) return;
        p = it->second;
        pending_.erase(it);
        auto s = subscriptions_.find(p.subscription);
        if (s != subscriptions_.end() && s->second.unacked > 0) s->second.unacked--;
    }
    p.queue->settle({p.tag}, ack ? broker::Settlement::Ack : broker::Settlement::Requeue);
    sendReceipt(f);
}

bool StompConnection::hasCredit(const std::string& subscription) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = subscriptions_.find(subscription);
    if (it == subscriptions_.end()) return false;
    return it->second.prefetch == 0 || it->second.unacked < it->second.prefetch;
}

void StompConnection::deliver(const std::string& subscription, broker::QueueBase& queue, broker::Delivery d) {
    StompFrame m;
    m.command = "MESSAGE";
    std::string ackId;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = subscriptions_.find(subscription);
        if (it == subscriptions_.end()) return;
        m.set("subscription", escapeHeader(subscription));
        m.set("destination", escapeHeader(it->second.destination));
        if (it->second.ackMode != "auto") {
            ackId = std::to_string(nextAckId_++);
            pending_[ackId] = Pending{subscription, it->second.queue, d.queueTag};
            it->second.unacked++;
            m.set("ack", ackId);
        }
    }
    m.set("message-id", ackId.empty() ? std::to_string(d.msg->id) : ackId);
    m.set("redelivered", d.redelivered ? "true" : "false");
    if (d.msg->props.contentType) m.set("content-type", escapeHeader(*d.msg->props.contentType));
    if (d.msg->props.correlationId) m.set("correlation-id", escapeHeader(*d.msg->props.correlationId));
    if (d.msg->props.replyTo) m.set("reply-to", escapeHeader(*d.msg->props.replyTo));
    if (d.msg->props.headers) {
        for (const auto& [k, v] : d.msg->props.headers->entries()) {
            if (auto s = v.asString()) m.set(k, *s);
            else if (auto i = v.asInt()) m.set(k, std::to_string(*i));
        }
    }
    m.body = d.msg->body;
    m.set("content-length", std::to_string(m.body.size()));
    (void)queue;
    tcp_->send(m.encode());
}

void StompConnection::subscriptionCancelled(const std::string& subscription, const std::string& reason) {
    sendError("subscription cancelled", "subscription '" + subscription + "' was cancelled: " + reason);
}

void StompConnection::cleanup() {
    std::map<std::string, Subscription> subs;
    std::map<std::string, Pending> pending;
    {
        std::lock_guard<std::mutex> lock(mu_);
        subs.swap(subscriptions_);
        pending.swap(pending_);
    }
    for (auto& [id, p] : pending) p.queue->settle({p.tag}, broker::Settlement::Requeue);
    for (auto& [id, s] : subs) {
        s.queue->removeConsumer(s.consumer->tag(), tcp_->id());
        if (vhost_) vhost_->maybeAutoDelete(s.queue);
    }
    if (vhost_) vhost_->deleteExclusiveQueues(tcp_->id());
    if (tokenAuth_) {
        tokenAuth_ = false;
        server_.broker().users().dropTokenSession(user_);
    }
}

void StompConnection::onClosed() {
    closed_ = true;
    cleanup();
}

}  // namespace tq::stomp
