// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "streamproto/stream_server.h"

#include <algorithm>

#include "broker/broker.h"
#include "broker/message.h"
#include "broker/stream_queue.h"
#include "broker/vhost.h"
#include "common/clock.h"
#include "common/logging.h"
#include "common/platform.h"
#include "common/strings.h"
#include "security/users.h"
#include "storage/stream_log.h"

namespace tq::streamproto {

using security::Access;

namespace {

class StreamHandler : public net::ConnectionHandler {
public:
    explicit StreamHandler(std::shared_ptr<StreamConnection> c) : conn_(std::move(c)) {}
    void onData(const char* data, size_t len) override { conn_->onData(data, len); }
    void onClosed() override { conn_->onClosed(); }
    void onTick(int64_t nowMs) override { conn_->onTick(nowMs); }

private:
    std::shared_ptr<StreamConnection> conn_;
};

std::string sequenceKey(const std::string& vhost, const std::string& stream, const std::string& reference) {
    return vhost + '\x1f' + stream + '\x1f' + reference;
}

}  // namespace

// ------------------------------------------------------------------ StreamServer

StreamServer::StreamServer(broker::Broker& broker, StreamOptions options)
    : broker_(broker), options_(std::move(options)) {}

StreamServer::~StreamServer() { stop(); }

bool StreamServer::start(const std::string& host, uint16_t port, std::string* error) {
    net::TcpServerOptions opts;
    opts.ioThreads = 1;
    server_ = std::make_unique<net::TcpServer>(
        "stream",
        [this](const std::shared_ptr<net::TcpConnection>& tcp) -> std::unique_ptr<net::ConnectionHandler> {
            auto c = std::make_shared<StreamConnection>(*this, tcp);
            registerConnection(c);
            return std::make_unique<StreamHandler>(c);
        },
        opts);
    if (!server_->start(host, port, error)) return false;
    LOG_INFO("stream") << "stream protocol listening on " << host << ":" << server_->port();
    return true;
}

void StreamServer::stop() {
    if (server_) server_->stop();
}

uint16_t StreamServer::port() const { return server_ ? server_->port() : 0; }

std::optional<uint64_t> StreamServer::publisherSequence(const std::string& vhost, const std::string& stream,
                                                        const std::string& reference) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = sequences_.find(sequenceKey(vhost, stream, reference));
    if (it == sequences_.end()) return std::nullopt;
    return it->second;
}

void StreamServer::recordPublisherSequence(const std::string& vhost, const std::string& stream,
                                           const std::string& reference, uint64_t sequence) {
    std::lock_guard<std::mutex> lock(mu_);
    uint64_t& cur = sequences_[sequenceKey(vhost, stream, reference)];
    cur = std::max(cur, sequence);
}

void StreamServer::registerConnection(const std::shared_ptr<StreamConnection>& c) {
    std::lock_guard<std::mutex> lock(mu_);
    connections_.erase(std::remove_if(connections_.begin(), connections_.end(),
                                      [](const std::weak_ptr<StreamConnection>& w) { return w.expired(); }),
                       connections_.end());
    connections_.push_back(c);
}

void StreamServer::unregisterConnection(const StreamConnection* c) {
    std::lock_guard<std::mutex> lock(mu_);
    connections_.erase(std::remove_if(connections_.begin(), connections_.end(),
                                      [c](const std::weak_ptr<StreamConnection>& w) {
                                          auto s = w.lock();
                                          return !s || s.get() == c;
                                      }),
                       connections_.end());
}

std::vector<Json> StreamServer::connections() const {
    std::vector<std::shared_ptr<StreamConnection>> live;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& w : connections_) {
            if (auto c = w.lock()) live.push_back(c);
        }
    }
    std::vector<Json> out;
    for (const auto& c : live) out.push_back(c->info());
    return out;
}

size_t StreamServer::connectionCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    size_t n = 0;
    for (const auto& w : connections_) n += w.expired() ? 0 : 1;
    return n;
}

// -------------------------------------------------------------- StreamConnection

StreamConnection::StreamConnection(StreamServer& server, std::shared_ptr<net::TcpConnection> tcp)
    : server_(server), tcp_(std::move(tcp)), parser_(kHandshakeFrameMax) {
    openedAtMs_ = wallClockMs();
    lastReceivedMs_ = monotonicMs();
    lastSentMs_ = lastReceivedMs_;
}

StreamConnection::~StreamConnection() = default;

void StreamConnection::send(std::string bytes) {
    lastSentMs_ = monotonicMs();
    tcp_->send(std::move(bytes));
}

void StreamConnection::respond(uint16_t command, uint32_t correlationId, uint16_t responseCode) {
    send(FrameWriter(command | key::ResponseFlag).u32(correlationId).u16(responseCode).finish());
}

void StreamConnection::closeWith(uint16_t responseCode, const std::string& reason) {
    if (state_ == State::Closing || state_ == State::Closed) return;
    LOG_WARN("stream") << tcp_->name() << ": closing connection: " << responseCodeName(responseCode) << " - "
                       << reason;
    send(FrameWriter(key::Close).u32(0).u16(responseCode).string(reason.substr(0, 255)).finish());
    state_ = State::Closing;
    tcp_->closeAfterFlush();
}

bool StreamConnection::canAccess(int access, const std::string& resource) const {
    return server_.broker().users().checkResource(user_, vhostName_, static_cast<Access>(access), resource);
}

broker::QueuePtr StreamConnection::findStream(const std::string& name) const {
    if (!vhost_) return nullptr;
    auto q = vhost_->findQueue(name);
    if (!q || q->type() != broker::QueueType::Stream) return nullptr;
    return q;
}

void StreamConnection::onData(const char* data, size_t len) {
    std::lock_guard<std::mutex> lock(infoMu_);
    if (state_ == State::Closed) return;
    lastReceivedMs_ = monotonicMs();
    parser_.feed(data, len);
    try {
        while (state_ != State::Closed) {
            auto f = parser_.next();
            if (!f) break;
            handle(*f);
        }
    } catch (const DecodeError& e) {
        closeWith(code::FrameTooLarge, e.what());
        state_ = State::Closed;
    }
    pump();
}

void StreamConnection::onTick(int64_t) {
    std::lock_guard<std::mutex> lock(infoMu_);
    if (state_ == State::Closed || state_ == State::Closing) return;
    int64_t now = monotonicMs();
    if (state_ != State::Open && now - lastReceivedMs_ > server_.broker().config().handshakeTimeoutMs) {
        closeWith(code::AuthenticationFailure, "handshake timeout");
        return;
    }
    if (heartbeatSec_ > 0) {
        int64_t interval = static_cast<int64_t>(heartbeatSec_) * 1000;
        if (now - lastReceivedMs_ > 2 * interval) {
            LOG_WARN("stream") << tcp_->name() << ": missed heartbeats from client, closing";
            state_ = State::Closed;
            tcp_->close();
            return;
        }
        if (now - lastSentMs_ > interval / 2) send(FrameWriter(key::Heartbeat).finish());
    }
    pump();
}

void StreamConnection::onClosed() {
    std::lock_guard<std::mutex> lock(infoMu_);
    state_ = State::Closed;
    publishers_.clear();
    subscriptions_.clear();
    if (vhost_) vhost_->connectionCount()--;
    vhost_.reset();
    if (tokenAuth_) server_.broker().users().dropTokenSession(user_);
    tokenAuth_ = false;
    server_.unregisterConnection(this);
}

Json StreamConnection::info() const {
    std::lock_guard<std::mutex> lock(infoMu_);
    Json j = Json::object();
    j["name"] = tcp_->name();
    j["user"] = user_;
    j["vhost"] = vhostName_;
    j["protocol"] = "stream";
    j["peer_host"] = tcp_->peerAddress();
    j["connected_at"] = openedAtMs_;
    j["frame_max"] = static_cast<int64_t>(frameMax_);
    j["heartbeat"] = static_cast<int64_t>(heartbeatSec_);
    j["publishers"] = static_cast<int64_t>(publishers_.size());
    j["subscriptions"] = static_cast<int64_t>(subscriptions_.size());
    j["messages_in"] = static_cast<int64_t>(messagesIn_.load());
    j["messages_out"] = static_cast<int64_t>(messagesOut_.load());
    Json props = Json::object();
    for (const auto& [k, v] : clientProperties_) props[k] = v;
    j["client_properties"] = props;
    return j;
}

// ------------------------------------------------------------------ dispatch

void StreamConnection::handle(const Frame& f) {
    FrameReader r(f.payload);
    uint16_t cmd = f.command();
    if (cmd == key::Heartbeat) return;
    if (f.isResponse()) {
        // Responses to server-initiated commands (close, metadata update).
        if (cmd == key::Close) {
            state_ = State::Closed;
            tcp_->closeAfterFlush();
        }
        return;
    }
    if (state_ != State::Open) {
        if (state_ == State::AwaitingOpen && cmd == key::Open) {
            handleOpen(f, r);
        } else if (state_ != State::Closing) {
            handleHandshake(f, r);
        }
        return;
    }
    switch (cmd) {
        case key::DeclarePublisher: onDeclarePublisher(r); break;
        case key::Publish: onPublish(f, r); break;
        case key::QueryPublisherSequence: onQueryPublisherSequence(r); break;
        case key::DeletePublisher: onDeletePublisher(r); break;
        case key::Subscribe: onSubscribe(r); break;
        case key::Credit: onCredit(r); break;
        case key::StoreOffset: onStoreOffset(r); break;
        case key::QueryOffset: onQueryOffset(r); break;
        case key::Unsubscribe: onUnsubscribe(r); break;
        case key::Create: onCreate(r); break;
        case key::Delete: onDelete(r); break;
        case key::Metadata: onMetadata(r); break;
        case key::StreamStats: onStreamStats(r); break;
        case key::Partitions: onPartitions(r); break;
        case key::Route: onRoute(r); break;
        case key::Close: onClose(r); break;
        default:
            closeWith(code::UnknownFrame, std::string("unknown command ") + std::to_string(cmd));
            break;
    }
}

void StreamConnection::handleHandshake(const Frame& f, FrameReader& r) {
    uint16_t cmd = f.command();
    auto& broker = server_.broker();
    switch (state_) {
        case State::AwaitingPeerProperties: {
            if (cmd != key::PeerProperties) return closeWith(code::UnknownFrame, "expected peer_properties");
            uint32_t corr = r.u32();
            clientProperties_ = r.map();
            StringMap mine{{"product", kProductName},
                           {"version", kProductVersion},
                           {"platform", std::string("C++/") + kPlatformName},
                           {"copyright", kCopyright},
                           {"information", "Licensed under the MIT License"},
                           {"cluster_name", broker.clusterName()}};
            send(FrameWriter(key::PeerProperties | key::ResponseFlag).u32(corr).u16(code::Ok).map(mine).finish());
            state_ = State::AwaitingSaslHandshake;
            return;
        }
        case State::AwaitingSaslHandshake: {
            if (cmd != key::SaslHandshake) return closeWith(code::UnknownFrame, "expected sasl_handshake");
            uint32_t corr = r.u32();
            std::vector<std::string> mechanisms;
            if (broker.authenticator().supports("PLAIN")) mechanisms.push_back("PLAIN");
            send(FrameWriter(key::SaslHandshake | key::ResponseFlag)
                     .u32(corr)
                     .u16(code::Ok)
                     .stringArray(mechanisms)
                     .finish());
            state_ = State::AwaitingSaslAuthenticate;
            return;
        }
        case State::AwaitingSaslAuthenticate: {
            if (cmd != key::SaslAuthenticate) return closeWith(code::UnknownFrame, "expected sasl_authenticate");
            uint32_t corr = r.u32();
            mechanism_ = r.string();
            std::string response = r.blob();
            if (mechanism_ != "PLAIN" || !broker.authenticator().supports(mechanism_)) {
                respond(key::SaslAuthenticate, corr, code::SaslMechanismNotSupported);
                return;
            }
            auto result = broker.authenticator().authenticate(mechanism_, response, tcp_->peerAddress());
            if (!result.ok) {
                LOG_WARN("stream") << tcp_->name() << ": authentication failed: " << result.error;
                bool loopback = result.error.find("localhost") != std::string::npos;
                respond(key::SaslAuthenticate, corr,
                        loopback ? code::AuthenticationFailureLoopback : code::AuthenticationFailure);
                state_ = State::Closing;
                tcp_->closeAfterFlush();
                return;
            }
            user_ = result.user;
            tokenAuth_ = result.token;
            respond(key::SaslAuthenticate, corr, code::Ok);
            send(FrameWriter(key::Tune).u32(server_.options().frameMax).u32(server_.options().heartbeatSec).finish());
            state_ = State::AwaitingTune;
            return;
        }
        case State::AwaitingTune: {
            if (cmd != key::Tune) return closeWith(code::UnknownFrame, "expected tune");
            uint32_t clientFrameMax = r.u32();
            uint32_t clientHeartbeat = r.u32();
            uint32_t serverFrameMax = server_.options().frameMax;
            frameMax_ = clientFrameMax == 0 ? serverFrameMax : std::min(clientFrameMax, serverFrameMax);
            uint32_t serverHeartbeat = server_.options().heartbeatSec;
            heartbeatSec_ = (clientHeartbeat == 0 || serverHeartbeat == 0) ? 0
                                                                           : std::min(clientHeartbeat, serverHeartbeat);
            parser_.setFrameMax(frameMax_);
            state_ = State::AwaitingOpen;
            return;
        }
        default:
            closeWith(code::UnknownFrame, std::string("unexpected ") + commandName(cmd) + " during handshake");
    }
}

void StreamConnection::handleOpen(const Frame&, FrameReader& r) {
    auto& broker = server_.broker();
    uint32_t corr = r.u32();
    std::string vhost = r.string();
    auto vh = broker.vhost(vhost);
    if (!vh || !broker.users().canAccessVhost(user_, vhost)) {
        respond(key::Open, corr, code::VirtualHostAccessFailure);
        state_ = State::Closing;
        tcp_->closeAfterFlush();
        return;
    }
    vhost_ = vh;
    vhostName_ = vhost;
    vh->connectionCount()++;
    std::string host = server_.options().advertisedHost.empty() ? "localhost" : server_.options().advertisedHost;
    uint16_t port = server_.options().advertisedPort ? server_.options().advertisedPort : tcp_->localPort();
    StringMap props{{"advertised_host", host}, {"advertised_port", std::to_string(port)}};
    send(FrameWriter(key::Open | key::ResponseFlag).u32(corr).u16(code::Ok).map(props).finish());
    state_ = State::Open;
    LOG_INFO("stream") << "accepted stream connection " << tcp_->name() << " (user '" << user_ << "', vhost '"
                       << vhostName_ << "')";
    broker.events().emit(vhostName_, "connection.created",
                         broker::eventDetails({{"name", tcp_->name()}, {"user", user_}, {"protocol", "stream"}}));
}

// ---------------------------------------------------------------- publishing

void StreamConnection::onDeclarePublisher(FrameReader& r) {
    uint32_t corr = r.u32();
    uint8_t id = r.u8();
    std::string reference = r.string();
    std::string stream = r.string();
    if (publishers_.count(id)) return respond(key::DeclarePublisher, corr, code::PreconditionFailed);
    if (!canAccess(static_cast<int>(Access::Write), stream)) {
        return respond(key::DeclarePublisher, corr, code::AccessRefused);
    }
    auto q = findStream(stream);
    if (!q) return respond(key::DeclarePublisher, corr, code::StreamDoesNotExist);
    if (!reference.empty()) {
        for (const auto& [pid, p] : publishers_) {
            if (p.reference == reference && p.stream == stream) {
                return respond(key::DeclarePublisher, corr, code::PreconditionFailed);
            }
        }
    }
    Publisher p;
    p.id = id;
    p.stream = stream;
    p.reference = reference;
    p.queue = q;
    if (!reference.empty()) {
        if (auto seq = server_.publisherSequence(vhostName_, stream, reference)) {
            p.lastSequence = *seq;
            p.hasSequence = true;
        }
    }
    publishers_[id] = std::move(p);
    respond(key::DeclarePublisher, corr, code::Ok);
}

void StreamConnection::onPublish(const Frame& f, FrameReader& r) {
    uint8_t id = r.u8();
    int32_t count = r.i32();
    if (count < 0) return closeWith(code::PreconditionFailed, "negative message count");
    auto it = publishers_.find(id);
    std::vector<uint64_t> confirmed;
    std::vector<uint64_t> failed;
    for (int32_t i = 0; i < count; ++i) {
        uint64_t publishingId = r.u64();
        std::string body = r.blob();
        if (it == publishers_.end()) {
            failed.push_back(publishingId);
            continue;
        }
        Publisher& p = it->second;
        if (!p.reference.empty() && p.hasSequence && publishingId < p.lastSequence) {
            confirmed.push_back(publishingId);
            continue;
        }
        amqp::BasicProperties props;
        props.deliveryMode = 2;
        auto msg = broker::makeMessage("", p.stream, std::move(props), std::move(body));
        if (p.queue->enqueue(msg) != broker::QueueBase::EnqueueResult::Accepted) {
            failed.push_back(publishingId);
            continue;
        }
        p.published++;
        messagesIn_++;
        if (!p.reference.empty()) {
            p.lastSequence = std::max(p.lastSequence, publishingId);
            p.hasSequence = true;
            server_.recordPublisherSequence(vhostName_, p.stream, p.reference, publishingId);
        }
        confirmed.push_back(publishingId);
    }
    (void)f;
    if (!confirmed.empty()) {
        FrameWriter w(key::PublishConfirm);
        w.u8(id).u32(static_cast<uint32_t>(confirmed.size()));
        for (uint64_t c : confirmed) w.u64(c);
        send(w.finish());
    }
    if (!failed.empty()) {
        uint16_t why = it == publishers_.end() ? code::PublisherDoesNotExist : code::InternalError;
        FrameWriter w(key::PublishError);
        w.u8(id).u32(static_cast<uint32_t>(failed.size()));
        for (uint64_t c : failed) w.u64(c).u16(why);
        send(w.finish());
    }
}

void StreamConnection::onQueryPublisherSequence(FrameReader& r) {
    uint32_t corr = r.u32();
    std::string reference = r.string();
    std::string stream = r.string();
    if (!findStream(stream)) {
        send(FrameWriter(key::QueryPublisherSequence | key::ResponseFlag)
                 .u32(corr)
                 .u16(code::StreamDoesNotExist)
                 .u64(0)
                 .finish());
        return;
    }
    uint64_t seq = server_.publisherSequence(vhostName_, stream, reference).value_or(0);
    send(FrameWriter(key::QueryPublisherSequence | key::ResponseFlag).u32(corr).u16(code::Ok).u64(seq).finish());
}

void StreamConnection::onDeletePublisher(FrameReader& r) {
    uint32_t corr = r.u32();
    uint8_t id = r.u8();
    if (!publishers_.erase(id)) return respond(key::DeletePublisher, corr, code::PublisherDoesNotExist);
    respond(key::DeletePublisher, corr, code::Ok);
}

// ---------------------------------------------------------------- consuming

void StreamConnection::onSubscribe(FrameReader& r) {
    uint32_t corr = r.u32();
    uint8_t id = r.u8();
    std::string stream = r.string();
    uint16_t spec = r.u16();
    uint64_t offset = 0;
    int64_t timestamp = 0;
    if (spec == offset_spec::Offset) offset = r.u64();
    else if (spec == offset_spec::Timestamp) timestamp = r.i64();
    uint16_t credit = r.u16();
    StringMap props;
    if (!r.atEnd()) props = r.map();

    if (subscriptions_.count(id)) return respond(key::Subscribe, corr, code::SubscriptionIdAlreadyExists);
    if (subscriptions_.size() >= server_.options().maxSubscriptions) {
        return respond(key::Subscribe, corr, code::PreconditionFailed);
    }
    if (!canAccess(static_cast<int>(Access::Read), stream)) return respond(key::Subscribe, corr, code::AccessRefused);
    if (!findStream(stream)) return respond(key::Subscribe, corr, code::StreamDoesNotExist);
    auto log = server_.broker().streams()->find(vhostName_, stream);
    if (!log) return respond(key::Subscribe, corr, code::StreamNotAvailable);

    Subscription s;
    s.id = id;
    s.stream = stream;
    s.log = log;
    s.credit = credit;
    s.properties = std::move(props);
    switch (spec) {
        case offset_spec::First: s.nextOffset = log->firstOffset(); break;
        case offset_spec::Last: s.nextOffset = log->nextOffset() > log->firstOffset() ? log->nextOffset() - 1 : log->nextOffset(); break;
        case offset_spec::Next: s.nextOffset = log->nextOffset(); break;
        case offset_spec::Offset: s.nextOffset = std::max(offset, log->firstOffset()); break;
        case offset_spec::Timestamp: s.nextOffset = log->offsetForTimestamp(timestamp); break;
        default: return respond(key::Subscribe, corr, code::PreconditionFailed);
    }
    subscriptions_[id] = std::move(s);
    respond(key::Subscribe, corr, code::Ok);
}

void StreamConnection::onCredit(FrameReader& r) {
    uint8_t id = r.u8();
    uint16_t credit = r.u16();
    auto it = subscriptions_.find(id);
    if (it == subscriptions_.end()) {
        send(FrameWriter(key::Credit | key::ResponseFlag).u16(code::SubscriptionIdDoesNotExist).u8(id).finish());
        return;
    }
    it->second.credit = std::min<uint32_t>(it->second.credit + credit, 0xffff);
}

void StreamConnection::onUnsubscribe(FrameReader& r) {
    uint32_t corr = r.u32();
    uint8_t id = r.u8();
    if (!subscriptions_.erase(id)) return respond(key::Unsubscribe, corr, code::SubscriptionIdDoesNotExist);
    respond(key::Unsubscribe, corr, code::Ok);
}

void StreamConnection::onStoreOffset(FrameReader& r) {
    std::string reference = r.string();
    std::string stream = r.string();
    uint64_t offset = r.u64();
    if (reference.empty() || !findStream(stream)) return;
    server_.broker().streams()->storeOffset(vhostName_, stream, reference, offset);
}

void StreamConnection::onQueryOffset(FrameReader& r) {
    uint32_t corr = r.u32();
    std::string reference = r.string();
    std::string stream = r.string();
    if (!findStream(stream)) {
        send(FrameWriter(key::QueryOffset | key::ResponseFlag).u32(corr).u16(code::StreamDoesNotExist).u64(0).finish());
        return;
    }
    auto offset = server_.broker().streams()->queryOffset(vhostName_, stream, reference);
    send(FrameWriter(key::QueryOffset | key::ResponseFlag)
             .u32(corr)
             .u16(offset ? code::Ok : code::NoOffset)
             .u64(offset.value_or(0))
             .finish());
}

void StreamConnection::pump() {
    if (state_ != State::Open) return;
    const uint32_t maxRecords = server_.options().maxRecordsPerChunk;
    for (auto& [id, s] : subscriptions_) {
        while (s.credit > 0) {
            if (s.nextOffset < s.log->firstOffset()) s.nextOffset = s.log->firstOffset();
            auto records = s.log->read(s.nextOffset, maxRecords);
            if (records.empty()) break;
            std::vector<std::string> bodies;
            bodies.reserve(records.size());
            size_t bytes = 0;
            int64_t ts = records.front().timestamp;
            uint64_t first = records.front().offset;
            for (const auto& rec : records) {
                std::string body;
                try {
                    auto msg = broker::Message::decode(rec.data);
                    body = msg ? msg->body : std::string();
                } catch (const DecodeError&) {
                    body.clear();
                }
                if (!bodies.empty() && bytes + body.size() + 64 > frameMax_) break;
                bytes += body.size() + 4;
                bodies.push_back(std::move(body));
            }
            send(FrameWriter(key::Deliver).u8(id).raw(encodeChunk(first, ts, bodies)).finish());
            s.nextOffset = first + bodies.size();
            s.delivered += bodies.size();
            messagesOut_ += bodies.size();
            s.credit--;
        }
    }
}

// -------------------------------------------------------------- management

void StreamConnection::onCreate(FrameReader& r) {
    uint32_t corr = r.u32();
    std::string stream = r.string();
    StringMap args = r.map();
    if (stream.empty() || str::startsWith(stream, "amq.")) return respond(key::Create, corr, code::PreconditionFailed);
    if (!canAccess(static_cast<int>(Access::Configure), stream)) return respond(key::Create, corr, code::AccessRefused);
    if (vhost_->findQueue(stream)) return respond(key::Create, corr, code::StreamAlreadyExists);

    broker::QueueDef def;
    def.name = stream;
    def.vhost = vhostName_;
    def.type = broker::QueueType::Stream;
    def.durable = true;
    def.arguments.set("x-queue-type", amqp::FieldValue::longString("stream"));
    for (const auto& [k, v] : args) {
        if (k == "max-length-bytes" || k == "stream-max-segment-size-bytes") {
            auto n = str::parseByteSize(v);
            if (!n || *n <= 0) return respond(key::Create, corr, code::PreconditionFailed);
            def.arguments.set("x-" + k, amqp::FieldValue::int64(*n));
        } else if (k == "max-age") {
            if (broker::StreamQueue::parseMaxAge(v) <= 0) return respond(key::Create, corr, code::PreconditionFailed);
            def.arguments.set("x-max-age", amqp::FieldValue::longString(v));
        } else if (k == "queue-leader-locator" || k == "initial-cluster-size") {
            def.arguments.set("x-" + k, amqp::FieldValue::longString(v));
        } else {
            return respond(key::Create, corr, code::PreconditionFailed);
        }
    }
    Json cmd = Json::object();
    cmd["op"] = "declare_queue";
    cmd["def"] = def.toJson();
    Json result = server_.broker().commitMetadata(cmd);
    respond(key::Create, corr, result["code"].asInt(0) == 0 ? code::Ok : code::PreconditionFailed);
}

void StreamConnection::onDelete(FrameReader& r) {
    uint32_t corr = r.u32();
    std::string stream = r.string();
    if (!canAccess(static_cast<int>(Access::Configure), stream)) return respond(key::Delete, corr, code::AccessRefused);
    if (!findStream(stream)) return respond(key::Delete, corr, code::StreamDoesNotExist);
    for (auto it = subscriptions_.begin(); it != subscriptions_.end();) {
        if (it->second.stream == stream) it = subscriptions_.erase(it);
        else ++it;
    }
    for (auto it = publishers_.begin(); it != publishers_.end();) {
        if (it->second.stream == stream) it = publishers_.erase(it);
        else ++it;
    }
    Json cmd = Json::object();
    cmd["op"] = "delete_queue";
    cmd["vhost"] = vhostName_;
    cmd["name"] = stream;
    Json result = server_.broker().commitMetadata(cmd);
    respond(key::Delete, corr, result["code"].asInt(0) == 0 ? code::Ok : code::InternalError);
}

void StreamConnection::onMetadata(FrameReader& r) {
    uint32_t corr = r.u32();
    std::vector<std::string> streams = r.stringArray();
    std::string host = server_.options().advertisedHost.empty() ? "localhost" : server_.options().advertisedHost;
    uint16_t port = server_.options().advertisedPort ? server_.options().advertisedPort : tcp_->localPort();
    FrameWriter w(key::Metadata | key::ResponseFlag);
    w.u32(corr);
    w.u32(1).u16(0).string(host).u32(port);
    w.u32(static_cast<uint32_t>(streams.size()));
    for (const auto& s : streams) {
        bool exists = findStream(s) != nullptr;
        w.string(s).u16(exists ? code::Ok : code::StreamDoesNotExist).u16(exists ? 0 : 0xffff).u32(0);
    }
    send(w.finish());
}

void StreamConnection::onStreamStats(FrameReader& r) {
    uint32_t corr = r.u32();
    std::string stream = r.string();
    if (!canAccess(static_cast<int>(Access::Read), stream)) {
        send(FrameWriter(key::StreamStats | key::ResponseFlag).u32(corr).u16(code::AccessRefused).u32(0).finish());
        return;
    }
    auto log = findStream(stream) ? server_.broker().streams()->find(vhostName_, stream) : nullptr;
    if (!log) {
        send(FrameWriter(key::StreamStats | key::ResponseFlag).u32(corr).u16(code::StreamDoesNotExist).u32(0).finish());
        return;
    }
    auto st = log->stats();
    int64_t committed = st.nextOffset > st.firstOffset ? static_cast<int64_t>(st.nextOffset) - 1 : -1;
    FrameWriter w(key::StreamStats | key::ResponseFlag);
    w.u32(corr).u16(code::Ok).u32(3);
    w.string("first_chunk_id").i64(static_cast<int64_t>(st.firstOffset));
    w.string("committed_chunk_id").i64(committed);
    w.string("segments").i64(static_cast<int64_t>(st.segments));
    send(w.finish());
}

void StreamConnection::onPartitions(FrameReader& r) {
    uint32_t corr = r.u32();
    std::string superStream = r.string();
    std::vector<std::string> partitions;
    for (const auto& b : vhost_->bindings()) {
        if (b.source == superStream && b.kind == broker::DestinationKind::Queue) partitions.push_back(b.destination);
    }
    if (partitions.empty()) {
        send(FrameWriter(key::Partitions | key::ResponseFlag).u32(corr).u16(code::StreamDoesNotExist).u32(0).finish());
        return;
    }
    std::sort(partitions.begin(), partitions.end(), [](const std::string& a, const std::string& b) {
        auto na = str::parseInt(a.substr(a.rfind('-') + 1));
        auto nb = str::parseInt(b.substr(b.rfind('-') + 1));
        if (na && nb) return *na < *nb;
        return a < b;
    });
    send(FrameWriter(key::Partitions | key::ResponseFlag).u32(corr).u16(code::Ok).stringArray(partitions).finish());
}

void StreamConnection::onRoute(FrameReader& r) {
    uint32_t corr = r.u32();
    std::string routingKey = r.string();
    std::string superStream = r.string();
    if (!vhost_->exchangeDef(superStream)) {
        send(FrameWriter(key::Route | key::ResponseFlag).u32(corr).u16(code::StreamDoesNotExist).u32(0).finish());
        return;
    }
    auto probe = broker::makeMessage(superStream, routingKey, {}, "");
    std::vector<std::string> streams;
    for (const auto& q : vhost_->route(superStream, probe).queues) streams.push_back(q->name());
    send(FrameWriter(key::Route | key::ResponseFlag).u32(corr).u16(code::Ok).stringArray(streams).finish());
}

void StreamConnection::onClose(FrameReader& r) {
    uint32_t corr = r.u32();
    uint16_t closingCode = r.u16();
    std::string reason = r.string();
    LOG_DEBUG("stream") << tcp_->name() << ": client closed (" << closingCode << " " << reason << ")";
    respond(key::Close, corr, code::Ok);
    state_ = State::Closing;
    tcp_->closeAfterFlush();
}

}  // namespace tq::streamproto
