// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "cluster/transport.h"

#include "common/bytes.h"
#include "common/crypto.h"
#include "common/logging.h"
#include "common/strings.h"

namespace tq::cluster {

bool parseNodeInfo(const std::string& text, NodeInfo& out) {
    size_t at = text.find('@');
    if (at == std::string::npos) return false;
    auto id = str::parseInt(text.substr(0, at));
    if (!id || *id <= 0) return false;
    auto ep = net::parseEndpoint(text.substr(at + 1), 25672);
    if (!ep) return false;
    out.id = static_cast<int>(*id);
    out.host = ep->host;
    out.port = ep->port;
    return true;
}

ClusterTransport::ClusterTransport(int selfId, std::string cookie, std::vector<NodeInfo> peers, Handler handler)
    : self_(selfId), cookie_(std::move(cookie)), handler_(std::move(handler)) {
    if (cookie_.empty()) cookie_ = "tropiqueue-cluster";
    for (auto& p : peers) {
        if (p.id == self_) continue;
        auto peer = std::make_unique<Peer>();
        peer->info = p;
        peers_[p.id] = std::move(peer);
    }
}

ClusterTransport::~ClusterTransport() { stop(); }

bool ClusterTransport::start(const std::string& host, uint16_t port, std::string* error) {
    listener_ = net::Socket::listenTcp(host, port, 64, error);
    if (!listener_.valid()) return false;
    port_ = listener_.localPort();
    running_.store(true);
    acceptThread_ = std::thread([this] { acceptLoop(); });
    for (auto& [id, peer] : peers_) {
        Peer* p = peer.get();
        p->thread = std::thread([this, p] { senderLoop(p); });
    }
    LOG_INFO("cluster") << "node " << self_ << " listening for peers on port " << port_;
    return true;
}

void ClusterTransport::stop() {
    if (!running_.exchange(false)) return;
    if (acceptThread_.joinable()) acceptThread_.join();
    listener_.close();
    for (auto& [id, peer] : peers_) {
        peer->cv.notify_all();
        if (peer->thread.joinable()) peer->thread.join();
    }
    std::vector<std::thread> readers;
    {
        std::lock_guard<std::mutex> lock(readersMu_);
        for (auto fd : readerSockets_) net::shutdownSocket(fd);
        readers.swap(readers_);
    }
    for (auto& t : readers) {
        if (t.joinable()) t.join();
    }
}

void ClusterTransport::send(int node, FrameType type, std::string payload) {
    auto it = peers_.find(node);
    if (it == peers_.end()) return;
    Peer* p = it->second.get();
    {
        std::lock_guard<std::mutex> lock(p->mu);
        if (p->queue.size() > 100000) p->queue.pop_front();
        p->queue.emplace_back(type, std::move(payload));
    }
    p->cv.notify_one();
}

bool ClusterTransport::isConnected(int node) const {
    auto it = peers_.find(node);
    return it != peers_.end() && it->second->connected.load();
}

std::vector<int> ClusterTransport::connectedPeers() const {
    std::vector<int> out;
    for (const auto& [id, p] : peers_) {
        if (p->connected.load()) out.push_back(id);
    }
    return out;
}

bool ClusterTransport::writeFrame(net::Socket& sock, FrameType type, const std::string& payload) {
    char header[5];
    storeBe32(header, static_cast<uint32_t>(payload.size() + 1));
    header[4] = static_cast<char>(type);
    return sock.sendAll(header, 5) && sock.sendAll(payload.data(), payload.size());
}

bool ClusterTransport::readFrame(net::Socket& sock, FrameType& type, std::string& payload) {
    char header[5];
    if (!sock.recvAll(header, 5)) return false;
    uint32_t len = loadBe32(header);
    if (len == 0) return false;
    type = static_cast<FrameType>(static_cast<uint8_t>(header[4]));
    payload.assign(len - 1, '\0');
    return len == 1 || sock.recvAll(payload.data(), len - 1);
}

void ClusterTransport::senderLoop(Peer* peer) {
    net::Socket sock;
    int backoffMs = 100;
    while (running_.load()) {
        if (!sock.valid()) {
            std::string err;
            sock = net::Socket::connectTcp(peer->info.host, peer->info.port, 2000, &err);
            if (!sock.valid()) {
                peer->connected.store(false);
                std::unique_lock<std::mutex> lock(peer->mu);
                peer->cv.wait_for(lock, std::chrono::milliseconds(backoffMs), [this] { return !running_.load(); });
                backoffMs = std::min(backoffMs * 2, 5000);
                continue;
            }
            ByteWriter hello;
            hello.i32(self_);
            hello.longString(cookie_);
            if (!writeFrame(sock, FrameType::Hello, hello.data())) {
                sock.close();
                continue;
            }
            backoffMs = 100;
            peer->connected.store(true);
            LOG_INFO("cluster") << "connected to peer " << peer->info.toString();
        }
        std::pair<FrameType, std::string> item;
        {
            std::unique_lock<std::mutex> lock(peer->mu);
            peer->cv.wait_for(lock, std::chrono::milliseconds(500),
                              [&] { return !running_.load() || !peer->queue.empty(); });
            if (!running_.load()) break;
            if (peer->queue.empty()) {
                item = {FrameType::Ping, std::string()};
            } else {
                item = std::move(peer->queue.front());
                peer->queue.pop_front();
            }
        }
        if (!writeFrame(sock, item.first, item.second)) {
            LOG_WARN("cluster") << "lost connection to peer " << peer->info.toString();
            peer->connected.store(false);
            sock.close();
        }
    }
    peer->connected.store(false);
}

void ClusterTransport::acceptLoop() {
    while (running_.load()) {
        std::vector<net::PollItem> items{{listener_.fd(), net::kPollIn, 0}};
        if (net::pollSockets(items, 200) <= 0) continue;
        std::string peer;
        uint16_t peerPort = 0;
        net::Socket s = listener_.accept(&peer, &peerPort);
        if (!s.valid()) continue;
        std::lock_guard<std::mutex> lock(readersMu_);
        readerSockets_.push_back(s.fd());
        readers_.emplace_back([this, sock = std::move(s), peer]() mutable { readLoop(std::move(sock), peer); });
    }
}

void ClusterTransport::readLoop(net::Socket sock, std::string peerAddr) {
    FrameType type;
    std::string payload;
    if (!readFrame(sock, type, payload) || type != FrameType::Hello) {
        LOG_WARN("cluster") << "rejecting peer connection from " << peerAddr << ": no hello";
        return;
    }
    int from = 0;
    std::string cookie;
    try {
        ByteReader r(payload);
        from = r.i32();
        cookie = r.longString();
    } catch (const DecodeError&) {
        return;
    }
    if (!crypto::constantTimeEquals(cookie, cookie_)) {
        LOG_WARN("cluster") << "rejecting peer connection from " << peerAddr << ": cookie mismatch";
        return;
    }
    if (!peers_.count(from)) {
        LOG_WARN("cluster") << "rejecting connection from unknown node id " << from << " (" << peerAddr << ")";
        return;
    }
    while (running_.load()) {
        if (!readFrame(sock, type, payload)) break;
        if (type == FrameType::Ping) continue;
        handler_(from, type, std::move(payload));
    }
}

}  // namespace tq::cluster
