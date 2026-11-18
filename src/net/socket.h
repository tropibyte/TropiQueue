// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include "common/platform.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#if defined(TQ_WINDOWS)
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

namespace tq::net {

#if defined(TQ_WINDOWS)
using socket_t = SOCKET;
constexpr socket_t kInvalidSocket = INVALID_SOCKET;
#else
using socket_t = int;
constexpr socket_t kInvalidSocket = -1;
#endif

// Ensures the platform socket library is initialised (WSAStartup on Windows).
void initSockets();

int lastSocketError();
bool isWouldBlock(int err);
std::string socketErrorString(int err);

struct Endpoint {
    std::string host;
    uint16_t port = 0;
    std::string toString() const;
};

// Parses "host:port", "[::1]:5672" or ":5672".
std::optional<Endpoint> parseEndpoint(const std::string& text, uint16_t defaultPort = 0);

// Thin RAII wrapper around a TCP socket descriptor.
class Socket {
public:
    Socket() = default;
    explicit Socket(socket_t fd) : fd_(fd) {}
    ~Socket();
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
    Socket(Socket&& other) noexcept : fd_(other.fd_) { other.fd_ = kInvalidSocket; }
    Socket& operator=(Socket&& other) noexcept;

    static Socket listenTcp(const std::string& host, uint16_t port, int backlog, std::string* error);
    static Socket connectTcp(const std::string& host, uint16_t port, int timeoutMs, std::string* error);

    // Returns an invalid socket when nothing is pending.
    Socket accept(std::string* peerAddress, uint16_t* peerPort);

    bool valid() const { return fd_ != kInvalidSocket; }
    socket_t fd() const { return fd_; }
    socket_t release();
    void close();

    bool setNonBlocking(bool on);
    bool setNoDelay(bool on);
    bool setKeepAlive(bool on);
    bool setReceiveTimeout(int ms);

    // Raw I/O. Return the byte count, 0 on orderly shutdown (recv), or -1 on
    // error (check lastSocketError()).
    long send(const void* data, size_t len);
    long recv(void* buf, size_t len);

    // Blocking helpers used by clients and inter-node links.
    bool sendAll(const void* data, size_t len);
    bool recvAll(void* buf, size_t len);

    uint16_t localPort() const;
    std::string peerAddress() const;

private:
    socket_t fd_ = kInvalidSocket;
};

// A connected pair of sockets used to wake up a poll loop from another thread.
bool makeSocketPair(Socket& a, Socket& b);

struct PollItem {
    socket_t fd = kInvalidSocket;
    short events = 0;
    short revents = 0;
};

constexpr short kPollIn = 0x0001;
constexpr short kPollOut = 0x0004;
constexpr short kPollErr = 0x0008;
constexpr short kPollHup = 0x0010;

// Waits for readiness on the given sockets. Returns the number of ready
// sockets, 0 on timeout, -1 on error.
int pollSockets(std::vector<PollItem>& items, int timeoutMs);

bool isLoopbackAddress(const std::string& address);

// Shuts down both directions of a socket without closing the descriptor.
void shutdownSocket(socket_t fd);

}  // namespace tq::net
