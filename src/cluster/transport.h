// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "net/socket.h"

namespace tq::cluster {

struct NodeInfo {
    int id = 0;
    std::string host;
    uint16_t port = 0;
    std::string toString() const { return std::to_string(id) + "@" + host + ":" + std::to_string(port); }
};

// Parses "2@10.0.0.2:25672".
bool parseNodeInfo(const std::string& text, NodeInfo& out);

enum class FrameType : uint8_t {
    Hello = 1,
    Raft = 2,
    Forward = 3,
    ForwardReply = 4,
    ClassicPublish = 5,
    Ping = 6,
};

// Inter-node links. Every node keeps one outbound connection per peer and
// accepts inbound connections from peers. Frames are length-prefixed:
//   length(4) type(1) payload
// The first frame on a connection must be Hello{node-id, cookie}.
class ClusterTransport {
public:
    using Handler = std::function<void(int fromNode, FrameType type, std::string payload)>;

    ClusterTransport(int selfId, std::string cookie, std::vector<NodeInfo> peers, Handler handler);
    ~ClusterTransport();

    bool start(const std::string& host, uint16_t port, std::string* error);
    void stop();
    uint16_t port() const { return port_; }

    // Queues a frame for a peer; never blocks on the network.
    void send(int node, FrameType type, std::string payload);
    bool isConnected(int node) const;
    std::vector<int> connectedPeers() const;

private:
    struct Peer {
        NodeInfo info;
        std::mutex mu;
        std::condition_variable cv;
        std::deque<std::pair<FrameType, std::string>> queue;
        std::atomic<bool> connected{false};
        std::thread thread;
    };

    void acceptLoop();
    void readLoop(net::Socket sock, std::string peerAddr);
    void senderLoop(Peer* peer);
    bool writeFrame(net::Socket& sock, FrameType type, const std::string& payload);
    bool readFrame(net::Socket& sock, FrameType& type, std::string& payload);

    int self_;
    std::string cookie_;
    Handler handler_;
    std::map<int, std::unique_ptr<Peer>> peers_;
    net::Socket listener_;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::thread acceptThread_;
    std::mutex readersMu_;
    std::vector<std::thread> readers_;
    std::vector<net::socket_t> readerSockets_;
};

}  // namespace tq::cluster
