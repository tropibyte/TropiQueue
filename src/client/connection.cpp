// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "client/connection.h"

#include <algorithm>
#include <cstring>

#include "amqp/constants.h"
#include "client/channel.h"
#include "common/logging.h"
#include "common/platform.h"

namespace tq::client {

using namespace tq::amqp;

std::shared_ptr<Connection> Connection::open(const std::string& uri, std::string* error) {
    auto params = parseUri(uri, error);
    if (!params) return nullptr;
    return open(*params, error);
}

std::shared_ptr<Connection> Connection::open(const ConnectionParams& params, std::string* error) {
    std::shared_ptr<Connection> c(new Connection());
    LOG_DEBUG("client") << "connecting to " << params.toString();
    std::string err;
    c->sock_ = net::Socket::connectTcp(params.host, params.port, params.connectTimeoutMs, &err);
    if (!c->sock_.valid()) {
        if (error) *error = err;
        return nullptr;
    }
    c->sock_.setReceiveTimeout(params.connectTimeoutMs);
    if (!c->handshake(params, error)) return nullptr;
    c->sock_.setReceiveTimeout(0);
    c->open_.store(true);
    c->reader_ = std::thread([c] { c->readerLoop(); });
    if (c->heartbeatSec_ > 0) c->heartbeat_ = std::thread([c] { c->heartbeatLoop(); });
    return c;
}

Connection::~Connection() {
    close();
}

Frame Connection::readFrameBlocking() {
    char buf[16384];
    while (true) {
        if (auto f = parser_.next()) return *f;
        long n = sock_.recv(buf, sizeof buf);
        if (n <= 0) throw ClientError(kConnectionForced, "connection closed by broker");
        parser_.feed(buf, static_cast<size_t>(n));
    }
}

bool Connection::handshake(const ConnectionParams& params, std::string* error) {
    auto fail = [&](const std::string& why) {
        if (error) *error = why;
        sock_.close();
        return false;
    };
    if (!sock_.sendAll(kProtocolHeader, sizeof kProtocolHeader)) return fail("cannot send protocol header");
    try {
        Frame f = readFrameBlocking();
        Method m = decodeMethod(f.payload);
        auto* start = methodAs<ConnectionStart>(m);
        if (!start) return fail("expected connection.start, got " + methodName(m));
        serverProperties_ = start->serverProperties;

        ConnectionStartOk ok;
        FieldTable caps;
        for (const char* cap : {"publisher_confirms", "exchange_exchange_bindings", "basic.nack",
                                "consumer_cancel_notify", "connection.blocked", "authentication_failure_close"}) {
            caps.set(cap, FieldValue::boolean(true));
        }
        ok.clientProperties.set("product", FieldValue::longString("TropiQueue C++ client"));
        ok.clientProperties.set("version", FieldValue::longString(kProductVersion));
        ok.clientProperties.set("platform", FieldValue::longString(std::string("C++/") + kPlatformName));
        ok.clientProperties.set("capabilities", FieldValue::fromTable(caps));
        if (!params.connectionName.empty())
            ok.clientProperties.set("connection_name", FieldValue::longString(params.connectionName));
        ok.mechanism = params.mechanism;
        if (params.mechanism == "AMQPLAIN") {
            ByteWriter w;
            w.shortString("LOGIN");
            w.u8('S');
            w.longString(params.user);
            w.shortString("PASSWORD");
            w.u8('S');
            w.longString(params.password);
            ok.response = w.take();
        } else {
            ok.response = std::string("\0", 1) + params.user + std::string("\0", 1) + params.password;
        }
        sendFrames(encodeMethodFrame(0, ok));

        f = readFrameBlocking();
        m = decodeMethod(f.payload);
        if (auto* close = methodAs<ConnectionClose>(m)) return fail(close->replyText);
        auto* tune = methodAs<ConnectionTune>(m);
        if (!tune) return fail("expected connection.tune, got " + methodName(m));
        auto negotiate = [](uint32_t server, uint32_t client) -> uint32_t {
            if (server == 0) return client;
            if (client == 0) return server;
            return std::min(server, client);
        };
        ConnectionTuneOk tuneOk;
        tuneOk.channelMax = static_cast<uint16_t>(negotiate(tune->channelMax, params.channelMax));
        tuneOk.frameMax = negotiate(tune->frameMax, params.frameMax);
        tuneOk.heartbeat = static_cast<uint16_t>(negotiate(tune->heartbeat, params.heartbeat));
        channelMax_ = tuneOk.channelMax;
        frameMax_ = params.frameMax;
        heartbeatSec_ = tuneOk.heartbeat;
        parser_.setFrameMax(tuneOk.frameMax);
        sendFrames(encodeMethodFrame(0, tuneOk));

        ConnectionOpen open;
        open.virtualHost = params.vhost;
        sendFrames(encodeMethodFrame(0, open));
        f = readFrameBlocking();
        m = decodeMethod(f.payload);
        if (auto* close = methodAs<ConnectionClose>(m)) return fail(close->replyText);
        if (!methodAs<ConnectionOpenOk>(m)) return fail("expected connection.open-ok, got " + methodName(m));
    } catch (const std::exception& e) {
        return fail(e.what());
    }
    return true;
}

void Connection::sendFrames(const std::string& bytes) {
    std::lock_guard<std::mutex> lock(writeMu_);
    if (!sock_.valid()) return;
    sock_.sendAll(bytes.data(), bytes.size());
}

void Connection::sendMethod(uint16_t channel, const Method& m) { sendFrames(encodeMethodFrame(channel, m)); }

std::shared_ptr<Channel> Connection::openChannel() {
    if (!open_.load()) throw ClientError(kChannelError, "connection is closed");
    std::shared_ptr<Channel> ch;
    {
        std::lock_guard<std::mutex> lock(channelsMu_);
        uint16_t id = nextChannel_++;
        if (channelMax_ != 0 && id > channelMax_) throw ClientError(kChannelError, "channel_max reached");
        ch = std::make_shared<Channel>(weak_from_this(), id);
        channels_[id] = ch;
    }
    ch->handleFrame(0, std::string());  // marks the channel as awaiting open-ok
    sendMethod(ch->id(), ChannelOpen{});
    return ch;
}

void Connection::readerLoop() {
    char buf[65536];
    while (open_.load()) {
        try {
            while (auto f = parser_.next()) dispatch(*f);
        } catch (const DecodeError& e) {
            failAll(kFrameError, e.what());
            break;
        }
        long n = sock_.recv(buf, sizeof buf);
        if (n <= 0) {
            failAll(kConnectionForced, closeReason().empty() ? "connection closed" : closeReason());
            break;
        }
        parser_.feed(buf, static_cast<size_t>(n));
    }
}

void Connection::heartbeatLoop() {
    std::unique_lock<std::mutex> lock(closeMu_);
    while (open_.load()) {
        closeCv_.wait_for(lock, std::chrono::milliseconds(heartbeatSec_ * 500));
        if (!open_.load()) break;
        lock.unlock();
        sendFrames(encodeHeartbeat());
        lock.lock();
    }
}

void Connection::dispatch(const Frame& f) {
    if (f.type == kFrameHeartbeat) return;
    if (f.channel == 0) {
        Method m = decodeMethod(f.payload);
        if (auto* close = methodAs<ConnectionClose>(m)) {
            {
                std::lock_guard<std::mutex> lock(closeMu_);
                closeReason_ = close->replyText;
            }
            closeCode_.store(close->replyCode);
            sendMethod(0, ConnectionCloseOk{});
            failAll(close->replyCode, close->replyText);
        } else if (methodAs<ConnectionCloseOk>(m)) {
            std::lock_guard<std::mutex> lock(closeMu_);
            closeOkReceived_ = true;
            closeCv_.notify_all();
        } else if (methodAs<ConnectionBlocked>(m)) {
            blocked_.store(true);
        } else if (methodAs<ConnectionUnblocked>(m)) {
            blocked_.store(false);
        }
        return;
    }
    std::shared_ptr<Channel> ch;
    {
        std::lock_guard<std::mutex> lock(channelsMu_);
        auto it = channels_.find(f.channel);
        if (it != channels_.end()) ch = it->second;
    }
    if (ch) ch->handleFrame(f.type, f.payload);
}

void Connection::failAll(uint16_t code, const std::string& text) {
    open_.store(false);
    std::map<uint16_t, std::shared_ptr<Channel>> chans;
    {
        std::lock_guard<std::mutex> lock(channelsMu_);
        chans = channels_;
    }
    for (auto& [id, ch] : chans) ch->connectionClosed(code, text);
    closeCv_.notify_all();
}

std::string Connection::closeReason() const {
    std::lock_guard<std::mutex> lock(closeMu_);
    return closeReason_;
}

void Connection::close() {
    // Only the first caller tears the connection down; concurrent callers
    // (e.g. an owner thread and a watchdog) return immediately.
    if (closeCalled_.exchange(true)) return;
    if (open_.load()) {
        ConnectionClose close;
        close.replyCode = kReplySuccess;
        close.replyText = "Goodbye";
        sendMethod(0, close);
        std::unique_lock<std::mutex> lock(closeMu_);
        closeCv_.wait_for(lock, std::chrono::seconds(2), [this] { return closeOkReceived_ || !open_.load(); });
    }
    open_.store(false);
    closeCv_.notify_all();
    if (sock_.valid()) net::shutdownSocket(sock_.fd());
    if (reader_.joinable()) {
        if (reader_.get_id() == std::this_thread::get_id()) reader_.detach();
        else reader_.join();
    }
    if (heartbeat_.joinable()) heartbeat_.join();
    std::lock_guard<std::mutex> lock(writeMu_);
    sock_.close();
}

}  // namespace tq::client
