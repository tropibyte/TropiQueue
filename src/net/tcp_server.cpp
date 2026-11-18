// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "net/tcp_server.h"

#include <algorithm>

#include "common/clock.h"
#include "common/logging.h"

namespace tq::net {

// --------------------------------------------------------------- TcpConnection

TcpConnection::TcpConnection(uint64_t id, Socket sock, std::string peerAddr, uint16_t peerPort, IoThread* owner)
    : id_(id), sock_(std::move(sock)), peerAddr_(std::move(peerAddr)), peerPort_(peerPort), owner_(owner) {
    localPort_ = sock_.localPort();
    connectedAt_ = wallClockMs();
}

TcpConnection::~TcpConnection() = default;

std::string TcpConnection::name() const {
    return peerAddr_ + ":" + std::to_string(peerPort_) + " -> :" + std::to_string(localPort_);
}

void TcpConnection::send(std::string data) {
    if (closed_.load() || data.empty()) return;
    {
        std::lock_guard<std::mutex> lock(outMu_);
        if (out_.empty()) {
            out_ = std::move(data);
        } else {
            out_.append(data);
        }
    }
    if (owner_) owner_->wake();
}

void TcpConnection::closeAfterFlush() {
    closeAfterFlush_.store(true);
    if (owner_) owner_->wake();
}

void TcpConnection::close() {
    closed_.store(true);
    if (owner_) owner_->wake();
}

size_t TcpConnection::pendingOutputBytes() const {
    std::lock_guard<std::mutex> lock(outMu_);
    return out_.size();
}

// -------------------------------------------------------------------- IoThread

IoThread::IoThread(TcpServer* server, int index) : server_(server), index_(index) {
    readBuf_.resize(server->options_.readBufferSize);
}

IoThread::~IoThread() { stop(); }

void IoThread::start() {
    if (!makeSocketPair(wakeRead_, wakeWrite_)) {
        LOG_ERROR("net") << "io thread " << index_ << ": cannot create wake-up socket pair";
    }
    running_.store(true);
    thread_ = std::thread([this] { run(); });
}

void IoThread::stop() {
    if (!running_.exchange(false)) return;
    wake();
    if (thread_.joinable()) thread_.join();
    for (auto& c : conns_) {
        c->sock_.close();
        if (c->handler_) c->handler_->onClosed();
        c->handler_.reset();
    }
    conns_.clear();
}

void IoThread::adopt(std::shared_ptr<TcpConnection> conn) {
    {
        std::lock_guard<std::mutex> lock(pendingMu_);
        pending_.push_back(std::move(conn));
    }
    wake();
}

void IoThread::wake() {
    if (!wakeWrite_.valid()) return;
    char b = 1;
    wakeWrite_.send(&b, 1);
}

bool IoThread::flush(const std::shared_ptr<TcpConnection>& conn) {
    std::string pending;
    {
        std::lock_guard<std::mutex> lock(conn->outMu_);
        pending.swap(conn->out_);
    }
    if (pending.empty()) return true;
    long n = conn->sock_.send(pending.data(), pending.size());
    if (n < 0) {
        int err = lastSocketError();
        if (isWouldBlock(err)) {
            std::lock_guard<std::mutex> lock(conn->outMu_);
            conn->out_.insert(0, pending);
            return true;
        }
        LOG_DEBUG("net") << "send failed on " << conn->name() << ": " << socketErrorString(err);
        return false;
    }
    conn->bytesOut_.fetch_add(static_cast<uint64_t>(n));
    return true;
}

void IoThread::handleReadable(const std::shared_ptr<TcpConnection>& conn) {
    long n = conn->sock_.recv(readBuf_.data(), readBuf_.size());
    if (n == 0) {
        conn->closed_.store(true);
        return;
    }
    if (n < 0) {
        if (!isWouldBlock(lastSocketError())) conn->closed_.store(true);
        return;
    }
    conn->bytesIn_.fetch_add(static_cast<uint64_t>(n));
    if (conn->handler_) conn->handler_->onData(readBuf_.data(), static_cast<size_t>(n));
}

void IoThread::finish(const std::shared_ptr<TcpConnection>& conn) {
    conn->closed_.store(true);
    conn->sock_.close();
    if (conn->handler_) {
        conn->handler_->onClosed();
        conn->handler_.reset();
    }
    server_->releaseIpSlot(conn->peerAddress());
    server_->connectionClosed(conn);
}

void IoThread::run() {
    std::vector<PollItem> items;
    int64_t lastTick = monotonicMs();
    const int tickMs = server_->options_.tickIntervalMs;
    while (running_.load()) {
        {
            std::lock_guard<std::mutex> lock(pendingMu_);
            for (auto& c : pending_) {
                c->sock_.setNonBlocking(true);
                c->sock_.setNoDelay(true);
                conns_.push_back(c);
                if (c->handler_) c->handler_->onOpen(c);
            }
            pending_.clear();
        }

        items.clear();
        items.push_back(PollItem{wakeRead_.fd(), kPollIn, 0});
        for (auto& c : conns_) {
            short ev = 0;
            if (!c->readPaused()) ev |= kPollIn;
            if (c->pendingOutputBytes() > 0) ev |= kPollOut;
            items.push_back(PollItem{c->sock_.fd(), ev, 0});
        }

        int rc = pollSockets(items, tickMs);
        if (rc < 0 && !isWouldBlock(lastSocketError())) {
            LOG_WARN("net") << "poll failed: " << socketErrorString(lastSocketError());
        }

        if (items[0].revents & kPollIn) {
            char drain[256];
            while (wakeRead_.recv(drain, sizeof drain) > 0) {
            }
        }

        for (size_t i = 0; i < conns_.size(); ++i) {
            auto& c = conns_[i];
            short rev = items[i + 1].revents;
            if (c->closed_.load()) continue;
            if (rev & (kPollErr | kPollHup)) {
                if (!(rev & kPollIn)) {
                    c->closed_.store(true);
                    continue;
                }
            }
            if (rev & kPollIn) handleReadable(c);
            if (!c->closed_.load() && c->pendingOutputBytes() > 0) {
                if (!flush(c)) c->closed_.store(true);
            }
            if (c->closeAfterFlush_.load() && c->pendingOutputBytes() == 0) c->closed_.store(true);
        }

        int64_t now = monotonicMs();
        if (now - lastTick >= tickMs) {
            lastTick = now;
            int64_t wall = wallClockMs();
            for (auto& c : conns_) {
                if (!c->closed_.load() && c->handler_) c->handler_->onTick(wall);
            }
        }

        for (auto it = conns_.begin(); it != conns_.end();) {
            if ((*it)->closed_.load()) {
                auto c = *it;
                it = conns_.erase(it);
                if (!c->closeAfterFlush_.load() || c->pendingOutputBytes() == 0) {
                    finish(c);
                } else {
                    flush(c);
                    finish(c);
                }
            } else {
                ++it;
            }
        }
    }
}

// ------------------------------------------------------------------- TcpServer

TcpServer::TcpServer(std::string name, HandlerFactory factory, TcpServerOptions options)
    : name_(std::move(name)), factory_(std::move(factory)), options_(options) {}

TcpServer::~TcpServer() { stop(); }

bool TcpServer::start(const std::string& host, uint16_t port, std::string* error) {
    listener_ = Socket::listenTcp(host, port, options_.backlog, error);
    if (!listener_.valid()) return false;
    listener_.setNonBlocking(true);
    port_ = listener_.localPort();
    int n = std::max(1, options_.ioThreads);
    for (int i = 0; i < n; ++i) {
        ioThreads_.push_back(std::make_unique<IoThread>(this, i));
        ioThreads_.back()->start();
    }
    running_.store(true);
    acceptThread_ = std::thread([this] { acceptLoop(); });
    LOG_INFO("net") << name_ << " listening on " << host << ":" << port_;
    return true;
}

void TcpServer::stop() {
    if (!running_.exchange(false)) return;
    if (acceptThread_.joinable()) acceptThread_.join();
    listener_.close();
    for (auto& io : ioThreads_) io->stop();
    ioThreads_.clear();
    std::lock_guard<std::mutex> lock(connMu_);
    connections_.clear();
    perIp_.clear();
}

void TcpServer::acceptLoop() {
    while (running_.load()) {
        std::vector<PollItem> items{{listener_.fd(), kPollIn, 0}};
        int rc = pollSockets(items, 200);
        if (rc <= 0) continue;
        std::string peer;
        uint16_t peerPort = 0;
        Socket s = listener_.accept(&peer, &peerPort);
        if (!s.valid()) continue;

        {
            std::lock_guard<std::mutex> lock(connMu_);
            if (static_cast<int>(connections_.size()) >= options_.maxConnections) {
                LOG_WARN("net") << name_ << ": connection limit reached, rejecting " << peer;
                continue;
            }
            if (options_.maxConnectionsPerIp > 0 && perIp_[peer] >= options_.maxConnectionsPerIp) {
                LOG_WARN("net") << name_ << ": per-address connection limit reached for " << peer;
                continue;
            }
            perIp_[peer]++;
        }

        uint64_t id = nextId_.fetch_add(1);
        IoThread* io = ioThreads_[nextIo_++ % ioThreads_.size()].get();
        auto conn = std::make_shared<TcpConnection>(id, std::move(s), peer, peerPort, io);
        conn->handler_ = factory_(conn);
        {
            std::lock_guard<std::mutex> lock(connMu_);
            connections_[id] = conn;
        }
        io->adopt(conn);
    }
}

void TcpServer::releaseIpSlot(const std::string& ip) {
    std::lock_guard<std::mutex> lock(connMu_);
    auto it = perIp_.find(ip);
    if (it != perIp_.end()) it->second--;
}

void TcpServer::connectionClosed(const std::shared_ptr<TcpConnection>& conn) {
    std::lock_guard<std::mutex> lock(connMu_);
    connections_.erase(conn->id());
    auto it = perIp_.find(conn->peerAddress());
    if (it != perIp_.end()) {
        if (--it->second <= 0) perIp_.erase(it);
    }
}

size_t TcpServer::connectionCount() const {
    std::lock_guard<std::mutex> lock(connMu_);
    return connections_.size();
}

std::vector<std::shared_ptr<TcpConnection>> TcpServer::connections() const {
    std::vector<std::shared_ptr<TcpConnection>> out;
    std::lock_guard<std::mutex> lock(connMu_);
    for (const auto& [id, weak] : connections_) {
        if (auto c = weak.lock()) out.push_back(c);
    }
    return out;
}

std::shared_ptr<TcpConnection> TcpServer::findConnection(uint64_t id) const {
    std::lock_guard<std::mutex> lock(connMu_);
    auto it = connections_.find(id);
    return it == connections_.end() ? nullptr : it->second.lock();
}

}  // namespace tq::net
