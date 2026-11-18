// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// A small blocking AMQP 0-9-1 client used by the tools and the test-suite.
// It works with TropiQueue and other RabbitMQ-compatible brokers.
#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include "amqp/frame.h"
#include "amqp/methods.h"
#include "amqp/properties.h"
#include "client/uri.h"
#include "net/socket.h"

namespace tq::client {

class Channel;

class ClientError : public std::runtime_error {
public:
    ClientError(uint16_t code, const std::string& text) : std::runtime_error(text), code(code) {}
    uint16_t code;
};

class Connection : public std::enable_shared_from_this<Connection> {
public:
    static std::shared_ptr<Connection> open(const ConnectionParams& params, std::string* error = nullptr);
    static std::shared_ptr<Connection> open(const std::string& uri, std::string* error = nullptr);
    ~Connection();

    std::shared_ptr<Channel> openChannel();
    void close();
    bool isOpen() const { return open_.load(); }

    uint32_t frameMax() const { return frameMax_; }
    const amqp::FieldTable& serverProperties() const { return serverProperties_; }
    // Reason the broker gave for closing the connection, if any.
    std::string closeReason() const;
    uint16_t closeCode() const { return closeCode_.load(); }
    bool blocked() const { return blocked_.load(); }

    void sendFrames(const std::string& bytes);
    void sendMethod(uint16_t channel, const amqp::Method& m);

private:
    Connection() = default;
    bool handshake(const ConnectionParams& params, std::string* error);
    amqp::Frame readFrameBlocking();
    void readerLoop();
    void heartbeatLoop();
    void dispatch(const amqp::Frame& f);
    void failAll(uint16_t code, const std::string& text);

    net::Socket sock_;
    amqp::FrameParser parser_;
    std::mutex writeMu_;
    std::atomic<bool> open_{false};
    std::atomic<bool> closeCalled_{false};
    std::thread reader_;
    std::thread heartbeat_;
    uint32_t frameMax_ = 131072;
    uint16_t channelMax_ = 2047;
    uint16_t heartbeatSec_ = 0;
    amqp::FieldTable serverProperties_;
    std::atomic<uint16_t> closeCode_{0};
    std::atomic<bool> blocked_{false};
    mutable std::mutex closeMu_;
    std::string closeReason_;
    std::condition_variable closeCv_;
    bool closeOkReceived_ = false;

    std::mutex channelsMu_;
    std::map<uint16_t, std::shared_ptr<Channel>> channels_;
    uint16_t nextChannel_ = 1;
};

}  // namespace tq::client
