// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "amqp/field_table.h"
#include "amqp/frame.h"
#include "amqp/methods.h"
#include "common/json.h"
#include "net/tcp_server.h"

namespace tq::broker {

class Broker;
class Channel;
class VirtualHost;

// Server side of one AMQP 0-9-1 connection: handshake, authentication,
// tuning, heartbeats and channel multiplexing.
class AmqpConnection : public std::enable_shared_from_this<AmqpConnection> {
public:
    enum class State { AwaitingHeader, AwaitingStartOk, AwaitingTuneOk, AwaitingOpen, Open, Closing, Closed };

    AmqpConnection(Broker& broker, std::shared_ptr<net::TcpConnection> tcp);
    ~AmqpConnection();

    // Callbacks from the TCP layer (I/O thread).
    void onOpen();
    void onData(const char* data, size_t len);
    void onClosed();
    void onTick(int64_t nowMs);

    Broker& broker() { return broker_; }
    uint64_t id() const { return id_; }
    const std::string& user() const { return user_; }
    const std::string& vhostName() const { return vhostName_; }
    VirtualHost* vhost() { return vhost_.get(); }
    std::shared_ptr<VirtualHost> vhostPtr() { return vhost_; }
    uint32_t frameMax() const { return frameMax_; }
    std::string name() const;
    const std::string& peerAddress() const;
    State state() const { return state_.load(); }
    const amqp::FieldTable& clientProperties() const { return clientProperties_; }
    bool clientCapability(const std::string& name) const;

    // Thread-safe: queues raw frame bytes for sending.
    void sendRaw(std::string bytes);
    void sendMethod(uint16_t channel, const amqp::Method& m);
    // Server-initiated close (management API, user deletion, alarms).
    void closeWithError(uint16_t code, const std::string& text);
    // Sends channel.close for a soft error.
    void closeChannel(uint16_t channel, uint16_t code, const std::string& text, uint16_t classId, uint16_t methodId);

    Json info() const;
    size_t channelCount() const;

private:
    void startHandshake();
    void processFrames();
    void handleFrame(const amqp::Frame& f);
    void handleConnectionMethod(const amqp::Method& m);
    void handleChannelFrame(const amqp::Frame& f);
    void onStartOk(const amqp::ConnectionStartOk& m);
    void onTuneOk(const amqp::ConnectionTuneOk& m);
    void onUpdateSecret(const amqp::ConnectionUpdateSecret& m);
    void onOpenVhost(const amqp::ConnectionOpen& m);
    void closeAllChannels();
    void hardError(uint16_t code, const std::string& text, uint16_t classId = 0, uint16_t methodId = 0);
    void cleanup();

    Broker& broker_;
    std::shared_ptr<net::TcpConnection> tcp_;
    uint64_t id_;
    std::atomic<State> state_{State::AwaitingHeader};
    amqp::FrameParser parser_;

    std::string user_;
    bool tokenAuth_ = false;
    bool countedForUser_ = false;
    std::string vhostName_;
    std::shared_ptr<VirtualHost> vhost_;
    amqp::FieldTable clientProperties_;
    std::string mechanism_;

    uint16_t channelMax_ = 0;
    uint32_t frameMax_ = 131072;
    uint16_t heartbeat_ = 0;
    int64_t openedAtMs_ = 0;
    std::atomic<int64_t> lastActivityMs_{0};
    std::atomic<int64_t> lastSentMs_{0};
    bool blockedSent_ = false;
    bool cleanedUp_ = false;
    bool countedInVhost_ = false;

    mutable std::mutex channelsMu_;
    std::map<uint16_t, std::shared_ptr<Channel>> channels_;
};

// Adapter that plugs an AmqpConnection into the TCP server.
class AmqpHandler : public net::ConnectionHandler {
public:
    explicit AmqpHandler(std::shared_ptr<AmqpConnection> conn) : conn_(std::move(conn)) {}
    void onOpen(const std::shared_ptr<net::TcpConnection>&) override { conn_->onOpen(); }
    void onData(const char* data, size_t len) override { conn_->onData(data, len); }
    void onClosed() override { conn_->onClosed(); }
    void onTick(int64_t nowMs) override { conn_->onTick(nowMs); }

private:
    std::shared_ptr<AmqpConnection> conn_;
};

}  // namespace tq::broker
