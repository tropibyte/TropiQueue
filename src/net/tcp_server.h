// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "net/socket.h"

namespace tq::net {

class TcpConnection;
class IoThread;

// Protocol logic attached to an accepted connection. All callbacks run on the
// connection's I/O thread.
class ConnectionHandler {
public:
    virtual ~ConnectionHandler() = default;
    virtual void onOpen(const std::shared_ptr<TcpConnection>& conn) { (void)conn; }
    virtual void onData(const char* data, size_t len) = 0;
    virtual void onClosed() = 0;
    // Called roughly every tick interval; used for heartbeats and timeouts.
    virtual void onTick(int64_t nowMs) { (void)nowMs; }
};

using HandlerFactory = std::function<std::unique_ptr<ConnectionHandler>(const std::shared_ptr<TcpConnection>&)>;

// One accepted TCP connection. send() and close() may be called from any thread.
class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    TcpConnection(uint64_t id, Socket sock, std::string peerAddr, uint16_t peerPort, IoThread* owner);
    ~TcpConnection();

    uint64_t id() const { return id_; }
    const std::string& peerAddress() const { return peerAddr_; }
    uint16_t peerPort() const { return peerPort_; }
    uint16_t localPort() const { return localPort_; }
    std::string name() const;  // "10.0.0.5:51234 -> 10.0.0.1:5672"

    void send(std::string data);
    // Flushes pending output, then closes.
    void closeAfterFlush();
    // Closes immediately, discarding pending output.
    void close();
    bool isClosed() const { return closed_.load(); }

    size_t pendingOutputBytes() const;
    uint64_t bytesReceived() const { return bytesIn_.load(); }
    uint64_t bytesSent() const { return bytesOut_.load(); }
    int64_t connectedAtMs() const { return connectedAt_; }

    // Pause/resume reading (used for flow control / memory alarms).
    void setReadPaused(bool paused) { readPaused_.store(paused); }
    bool readPaused() const { return readPaused_.load(); }

private:
    friend class IoThread;
    friend class TcpServer;

    uint64_t id_;
    Socket sock_;
    std::string peerAddr_;
    uint16_t peerPort_;
    uint16_t localPort_ = 0;
    IoThread* owner_;
    mutable std::mutex outMu_;
    std::string out_;
    std::atomic<bool> closed_{false};
    std::atomic<bool> closeAfterFlush_{false};
    std::atomic<bool> readPaused_{false};
    std::atomic<uint64_t> bytesIn_{0};
    std::atomic<uint64_t> bytesOut_{0};
    int64_t connectedAt_ = 0;
    std::unique_ptr<ConnectionHandler> handler_;
};

struct TcpServerOptions {
    int ioThreads = 2;
    int maxConnections = 65536;
    int maxConnectionsPerIp = 0;  // 0 = unlimited
    int backlog = 128;
    int tickIntervalMs = 250;
    size_t readBufferSize = 64 * 1024;
};

class TcpServer {
public:
    TcpServer(std::string name, HandlerFactory factory, TcpServerOptions options = {});
    ~TcpServer();

    bool start(const std::string& host, uint16_t port, std::string* error);
    void stop();
    uint16_t port() const { return port_; }
    const std::string& name() const { return name_; }

    size_t connectionCount() const;
    std::vector<std::shared_ptr<TcpConnection>> connections() const;
    std::shared_ptr<TcpConnection> findConnection(uint64_t id) const;

private:
    friend class IoThread;
    void acceptLoop();
    void connectionClosed(const std::shared_ptr<TcpConnection>& conn);
    void releaseIpSlot(const std::string& ip);

    std::string name_;
    HandlerFactory factory_;
    TcpServerOptions options_;
    Socket listener_;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::thread acceptThread_;
    std::vector<std::unique_ptr<IoThread>> ioThreads_;
    std::atomic<uint64_t> nextId_{1};
    size_t nextIo_ = 0;

    mutable std::mutex connMu_;
    std::map<uint64_t, std::weak_ptr<TcpConnection>> connections_;
    std::map<std::string, int> perIp_;
};

// Poll loop that owns a subset of the server's connections.
class IoThread {
public:
    IoThread(TcpServer* server, int index);
    ~IoThread();

    void start();
    void stop();
    void adopt(std::shared_ptr<TcpConnection> conn);
    void wake();

private:
    void run();
    void handleReadable(const std::shared_ptr<TcpConnection>& conn);
    bool flush(const std::shared_ptr<TcpConnection>& conn);
    void finish(const std::shared_ptr<TcpConnection>& conn);

    TcpServer* server_;
    int index_;
    std::thread thread_;
    std::atomic<bool> running_{false};
    Socket wakeRead_, wakeWrite_;
    std::mutex pendingMu_;
    std::vector<std::shared_ptr<TcpConnection>> pending_;
    std::vector<std::shared_ptr<TcpConnection>> conns_;
    std::vector<char> readBuf_;
};

}  // namespace tq::net
