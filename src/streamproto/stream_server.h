// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Listener for the binary stream protocol (port 5552 by default). It gives
// stream clients a fast path to the same streams AMQP clients use through
// x-queue-type=stream queues: publishers append batches with deduplication
// by publishing id, and consumers receive chunks under credit-based flow
// control, starting from any offset or timestamp.
//
// Message payloads are stored as the body of a broker message, so AMQP
// 0-9-1 consumers of the stream receive exactly the bytes a stream client
// published (and vice versa).
#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "broker/queue.h"
#include "common/json.h"
#include "net/tcp_server.h"
#include "streamproto/stream_codec.h"

namespace tq::broker {
class Broker;
class VirtualHost;
}  // namespace tq::broker
namespace tq::storage {
class StreamLog;
}

namespace tq::streamproto {

struct StreamOptions {
    uint32_t frameMax = kDefaultFrameMax;
    uint32_t heartbeatSec = 60;
    std::string advertisedHost;  // defaults to the node's host name
    uint16_t advertisedPort = 0;  // defaults to the listener port
    uint32_t maxRecordsPerChunk = 256;
    // Publishers and subscriptions per connection (ids are one byte).
    uint16_t maxPublishers = 256;
    uint16_t maxSubscriptions = 256;
};

class StreamConnection;

class StreamServer {
public:
    StreamServer(broker::Broker& broker, StreamOptions options);
    ~StreamServer();

    bool start(const std::string& host, uint16_t port, std::string* error);
    void stop();
    uint16_t port() const;

    broker::Broker& broker() { return broker_; }
    const StreamOptions& options() const { return options_; }

    // Highest publishing id seen for a named publisher.
    std::optional<uint64_t> publisherSequence(const std::string& vhost, const std::string& stream,
                                              const std::string& reference) const;
    void recordPublisherSequence(const std::string& vhost, const std::string& stream, const std::string& reference,
                                 uint64_t sequence);

    void registerConnection(const std::shared_ptr<StreamConnection>& c);
    void unregisterConnection(const StreamConnection* c);
    std::vector<Json> connections() const;
    size_t connectionCount() const;

private:
    broker::Broker& broker_;
    StreamOptions options_;
    std::unique_ptr<net::TcpServer> server_;
    mutable std::mutex mu_;
    std::map<std::string, uint64_t> sequences_;
    std::vector<std::weak_ptr<StreamConnection>> connections_;
};

class StreamConnection : public std::enable_shared_from_this<StreamConnection> {
public:
    StreamConnection(StreamServer& server, std::shared_ptr<net::TcpConnection> tcp);
    ~StreamConnection();

    void onData(const char* data, size_t len);
    void onTick(int64_t nowMs);
    void onClosed();

    Json info() const;

private:
    enum class State {
        AwaitingPeerProperties,
        AwaitingSaslHandshake,
        AwaitingSaslAuthenticate,
        AwaitingTune,
        AwaitingOpen,
        Open,
        Closing,
        Closed,
    };

    struct Publisher {
        uint8_t id = 0;
        std::string stream;
        std::string reference;
        broker::QueuePtr queue;
        uint64_t published = 0;
        uint64_t lastSequence = 0;
        bool hasSequence = false;
    };

    struct Subscription {
        uint8_t id = 0;
        std::string stream;
        std::shared_ptr<storage::StreamLog> log;
        uint64_t nextOffset = 0;
        uint32_t credit = 0;
        StringMap properties;
        uint64_t delivered = 0;
    };

    void handle(const Frame& f);
    void handleHandshake(const Frame& f, FrameReader& r);
    void handleOpen(const Frame& f, FrameReader& r);

    void onDeclarePublisher(FrameReader& r);
    void onPublish(const Frame& f, FrameReader& r);
    void onQueryPublisherSequence(FrameReader& r);
    void onDeletePublisher(FrameReader& r);
    void onSubscribe(FrameReader& r);
    void onCredit(FrameReader& r);
    void onStoreOffset(FrameReader& r);
    void onQueryOffset(FrameReader& r);
    void onUnsubscribe(FrameReader& r);
    void onCreate(FrameReader& r);
    void onDelete(FrameReader& r);
    void onMetadata(FrameReader& r);
    void onStreamStats(FrameReader& r);
    void onPartitions(FrameReader& r);
    void onRoute(FrameReader& r);
    void onClose(FrameReader& r);

    void pump();
    void respond(uint16_t command, uint32_t correlationId, uint16_t responseCode);
    void send(std::string bytes);
    void closeWith(uint16_t responseCode, const std::string& reason);

    bool canAccess(int access, const std::string& resource) const;
    broker::QueuePtr findStream(const std::string& name) const;

    StreamServer& server_;
    std::shared_ptr<net::TcpConnection> tcp_;
    FrameParser parser_;
    State state_ = State::AwaitingPeerProperties;

    std::string user_;
    bool tokenAuth_ = false;
    std::string vhostName_;
    std::shared_ptr<broker::VirtualHost> vhost_;
    StringMap clientProperties_;
    std::string mechanism_;
    uint32_t frameMax_ = kDefaultFrameMax;
    uint32_t heartbeatSec_ = 0;
    int64_t lastReceivedMs_ = 0;
    int64_t lastSentMs_ = 0;
    int64_t openedAtMs_ = 0;

    std::map<uint8_t, Publisher> publishers_;
    std::map<uint8_t, Subscription> subscriptions_;
    std::atomic<uint64_t> messagesIn_{0};
    std::atomic<uint64_t> messagesOut_{0};
    mutable std::mutex infoMu_;
};

}  // namespace tq::streamproto
