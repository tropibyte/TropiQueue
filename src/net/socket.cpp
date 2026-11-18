// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "net/socket.h"

#include <cstring>
#include <mutex>

#include "common/strings.h"

#if defined(TQ_WINDOWS)
#if defined(_MSC_VER)
#pragma comment(lib, "ws2_32.lib")
#endif
#else
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace tq::net {

void initSockets() {
#if defined(TQ_WINDOWS)
    static std::once_flag once;
    std::call_once(once, [] {
        WSADATA data;
        WSAStartup(MAKEWORD(2, 2), &data);
    });
#endif
}

int lastSocketError() {
#if defined(TQ_WINDOWS)
    return WSAGetLastError();
#else
    return errno;
#endif
}

bool isWouldBlock(int err) {
#if defined(TQ_WINDOWS)
    return err == WSAEWOULDBLOCK || err == WSAEINPROGRESS;
#else
    return err == EAGAIN || err == EWOULDBLOCK || err == EINPROGRESS || err == EINTR;
#endif
}

std::string socketErrorString(int err) {
#if defined(TQ_WINDOWS)
    char buf[256] = {0};
    FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, nullptr, static_cast<DWORD>(err), 0, buf,
                   sizeof buf, nullptr);
    return str::trim(buf);
#else
    return std::strerror(err);
#endif
}

std::string Endpoint::toString() const {
    if (host.find(':') != std::string::npos) return "[" + host + "]:" + std::to_string(port);
    return host + ":" + std::to_string(port);
}

std::optional<Endpoint> parseEndpoint(const std::string& text, uint16_t defaultPort) {
    Endpoint ep;
    std::string t = str::trim(text);
    if (t.empty()) return std::nullopt;
    std::string portPart;
    if (t[0] == '[') {
        size_t close = t.find(']');
        if (close == std::string::npos) return std::nullopt;
        ep.host = t.substr(1, close - 1);
        if (close + 1 < t.size()) {
            if (t[close + 1] != ':') return std::nullopt;
            portPart = t.substr(close + 2);
        }
    } else {
        size_t colon = t.rfind(':');
        if (colon == std::string::npos) {
            if (auto p = str::parseUint(t)) {
                portPart = t;
            } else {
                ep.host = t;
            }
        } else {
            ep.host = t.substr(0, colon);
            portPart = t.substr(colon + 1);
        }
    }
    if (portPart.empty()) {
        ep.port = defaultPort;
    } else {
        auto p = str::parseUint(portPart);
        if (!p || *p > 65535) return std::nullopt;
        ep.port = static_cast<uint16_t>(*p);
    }
    if (ep.host.empty()) ep.host = "0.0.0.0";
    return ep;
}

Socket::~Socket() { close(); }

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = other.fd_;
        other.fd_ = kInvalidSocket;
    }
    return *this;
}

socket_t Socket::release() {
    socket_t fd = fd_;
    fd_ = kInvalidSocket;
    return fd;
}

void Socket::close() {
    if (fd_ == kInvalidSocket) return;
#if defined(TQ_WINDOWS)
    ::closesocket(fd_);
#else
    ::close(fd_);
#endif
    fd_ = kInvalidSocket;
}

static addrinfo* resolve(const std::string& host, uint16_t port, bool passive) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    if (passive) hints.ai_flags = AI_PASSIVE;
    addrinfo* res = nullptr;
    std::string service = std::to_string(port);
    const char* node = host.empty() ? nullptr : host.c_str();
    if (getaddrinfo(node, service.c_str(), &hints, &res) != 0) return nullptr;
    return res;
}

Socket Socket::listenTcp(const std::string& host, uint16_t port, int backlog, std::string* error) {
    initSockets();
    addrinfo* res = resolve(host, port, true);
    if (!res) {
        if (error) *error = "cannot resolve " + host;
        return Socket{};
    }
    Socket result;
    for (addrinfo* ai = res; ai; ai = ai->ai_next) {
        socket_t fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd == kInvalidSocket) continue;
        Socket s(fd);
        int one = 1;
#if defined(TQ_WINDOWS)
        ::setsockopt(fd, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, reinterpret_cast<const char*>(&one), sizeof one);
#else
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
#endif
        if (::bind(fd, ai->ai_addr, static_cast<int>(ai->ai_addrlen)) != 0) {
            if (error) *error = "bind failed: " + socketErrorString(lastSocketError());
            continue;
        }
        if (::listen(fd, backlog) != 0) {
            if (error) *error = "listen failed: " + socketErrorString(lastSocketError());
            continue;
        }
        result = std::move(s);
        break;
    }
    freeaddrinfo(res);
    return result;
}

Socket Socket::connectTcp(const std::string& host, uint16_t port, int timeoutMs, std::string* error) {
    initSockets();
    addrinfo* res = resolve(host, port, false);
    if (!res) {
        if (error) *error = "cannot resolve " + host;
        return Socket{};
    }
    Socket result;
    for (addrinfo* ai = res; ai; ai = ai->ai_next) {
        socket_t fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd == kInvalidSocket) continue;
        Socket s(fd);
        s.setNonBlocking(true);
        int rc = ::connect(fd, ai->ai_addr, static_cast<int>(ai->ai_addrlen));
        if (rc != 0 && !isWouldBlock(lastSocketError())) {
            if (error) *error = "connect failed: " + socketErrorString(lastSocketError());
            continue;
        }
        if (rc != 0) {
            std::vector<PollItem> items{{fd, kPollOut, 0}};
            int n = pollSockets(items, timeoutMs);
            if (n <= 0 || (items[0].revents & (kPollErr | kPollHup))) {
                if (error) *error = n == 0 ? "connect timed out" : "connection refused";
                continue;
            }
            int soErr = 0;
            socklen_t len = sizeof soErr;
            ::getsockopt(fd, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&soErr), &len);
            if (soErr != 0) {
                if (error) *error = "connect failed: " + socketErrorString(soErr);
                continue;
            }
        }
        s.setNonBlocking(false);
        s.setNoDelay(true);
        result = std::move(s);
        break;
    }
    freeaddrinfo(res);
    return result;
}

Socket Socket::accept(std::string* peerAddress, uint16_t* peerPort) {
    sockaddr_storage addr{};
    socklen_t len = sizeof addr;
    socket_t fd = ::accept(fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    if (fd == kInvalidSocket) return Socket{};
    char buf[INET6_ADDRSTRLEN] = {0};
    uint16_t port = 0;
    if (addr.ss_family == AF_INET) {
        auto* in = reinterpret_cast<sockaddr_in*>(&addr);
        inet_ntop(AF_INET, &in->sin_addr, buf, sizeof buf);
        port = ntohs(in->sin_port);
    } else if (addr.ss_family == AF_INET6) {
        auto* in6 = reinterpret_cast<sockaddr_in6*>(&addr);
        inet_ntop(AF_INET6, &in6->sin6_addr, buf, sizeof buf);
        port = ntohs(in6->sin6_port);
    }
    if (peerAddress) *peerAddress = buf;
    if (peerPort) *peerPort = port;
    return Socket(fd);
}

bool Socket::setNonBlocking(bool on) {
#if defined(TQ_WINDOWS)
    u_long mode = on ? 1 : 0;
    return ioctlsocket(fd_, FIONBIO, &mode) == 0;
#else
    int flags = fcntl(fd_, F_GETFL, 0);
    if (flags < 0) return false;
    flags = on ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
    return fcntl(fd_, F_SETFL, flags) == 0;
#endif
}

bool Socket::setNoDelay(bool on) {
    int v = on ? 1 : 0;
    return ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&v), sizeof v) == 0;
}

bool Socket::setKeepAlive(bool on) {
    int v = on ? 1 : 0;
    return ::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, reinterpret_cast<const char*>(&v), sizeof v) == 0;
}

bool Socket::setReceiveTimeout(int ms) {
#if defined(TQ_WINDOWS)
    DWORD tv = static_cast<DWORD>(ms);
    return ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&tv), sizeof tv) == 0;
#else
    timeval tv{};
    tv.tv_sec = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;
    return ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv) == 0;
#endif
}

long Socket::send(const void* data, size_t len) {
#if defined(TQ_WINDOWS)
    return ::send(fd_, static_cast<const char*>(data), static_cast<int>(len), 0);
#elif defined(MSG_NOSIGNAL)
    return static_cast<long>(::send(fd_, data, len, MSG_NOSIGNAL));
#else
    return static_cast<long>(::send(fd_, data, len, 0));
#endif
}

long Socket::recv(void* buf, size_t len) {
#if defined(TQ_WINDOWS)
    return ::recv(fd_, static_cast<char*>(buf), static_cast<int>(len), 0);
#else
    return static_cast<long>(::recv(fd_, buf, len, 0));
#endif
}

bool Socket::sendAll(const void* data, size_t len) {
    const char* p = static_cast<const char*>(data);
    while (len > 0) {
        long n = send(p, len);
        if (n < 0) {
            if (isWouldBlock(lastSocketError())) {
                std::vector<PollItem> items{{fd_, kPollOut, 0}};
                pollSockets(items, 1000);
                continue;
            }
            return false;
        }
        p += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

bool Socket::recvAll(void* buf, size_t len) {
    char* p = static_cast<char*>(buf);
    while (len > 0) {
        long n = recv(p, len);
        if (n <= 0) return false;
        p += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

uint16_t Socket::localPort() const {
    sockaddr_storage addr{};
    socklen_t len = sizeof addr;
    if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len) != 0) return 0;
    if (addr.ss_family == AF_INET) return ntohs(reinterpret_cast<sockaddr_in*>(&addr)->sin_port);
    if (addr.ss_family == AF_INET6) return ntohs(reinterpret_cast<sockaddr_in6*>(&addr)->sin6_port);
    return 0;
}

std::string Socket::peerAddress() const {
    sockaddr_storage addr{};
    socklen_t len = sizeof addr;
    if (::getpeername(fd_, reinterpret_cast<sockaddr*>(&addr), &len) != 0) return "";
    char buf[INET6_ADDRSTRLEN] = {0};
    if (addr.ss_family == AF_INET)
        inet_ntop(AF_INET, &reinterpret_cast<sockaddr_in*>(&addr)->sin_addr, buf, sizeof buf);
    else if (addr.ss_family == AF_INET6)
        inet_ntop(AF_INET6, &reinterpret_cast<sockaddr_in6*>(&addr)->sin6_addr, buf, sizeof buf);
    return buf;
}

bool makeSocketPair(Socket& a, Socket& b) {
#if defined(TQ_POSIX)
    int fds[2];
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0) {
        a = Socket(fds[0]);
        b = Socket(fds[1]);
        a.setNonBlocking(true);
        b.setNonBlocking(true);
        return true;
    }
#endif
    // Fallback for Windows: connect two TCP sockets through a temporary listener.
    std::string err;
    Socket listener = Socket::listenTcp("0.0.0.0", 0, 1, &err);
    if (!listener.valid()) return false;
    uint16_t port = listener.localPort();
    Socket client = Socket::connectTcp("127.0.0.1", port, 2000, &err);
    if (!client.valid()) return false;
    std::string peer;
    uint16_t peerPort = 0;
    Socket server = listener.accept(&peer, &peerPort);
    if (!server.valid()) return false;
    a = std::move(client);
    b = std::move(server);
    a.setNonBlocking(true);
    b.setNonBlocking(true);
    return true;
}

int pollSockets(std::vector<PollItem>& items, int timeoutMs) {
    if (items.empty()) return 0;
#if defined(TQ_WINDOWS)
    std::vector<WSAPOLLFD> fds(items.size());
    for (size_t i = 0; i < items.size(); ++i) {
        fds[i].fd = items[i].fd;
        fds[i].events = 0;
        if (items[i].events & kPollIn) fds[i].events |= POLLRDNORM;
        if (items[i].events & kPollOut) fds[i].events |= POLLWRNORM;
        fds[i].revents = 0;
    }
    int rc = WSAPoll(fds.data(), static_cast<ULONG>(fds.size()), timeoutMs);
    for (size_t i = 0; i < items.size(); ++i) {
        short r = 0;
        if (fds[i].revents & (POLLRDNORM | POLLRDBAND)) r |= kPollIn;
        if (fds[i].revents & POLLWRNORM) r |= kPollOut;
        if (fds[i].revents & (POLLERR | POLLNVAL)) r |= kPollErr;
        if (fds[i].revents & POLLHUP) r |= kPollHup;
        items[i].revents = r;
    }
    return rc;
#else
    std::vector<pollfd> fds(items.size());
    for (size_t i = 0; i < items.size(); ++i) {
        fds[i].fd = items[i].fd;
        fds[i].events = 0;
        if (items[i].events & kPollIn) fds[i].events |= POLLIN;
        if (items[i].events & kPollOut) fds[i].events |= POLLOUT;
        fds[i].revents = 0;
    }
    int rc = ::poll(fds.data(), static_cast<nfds_t>(fds.size()), timeoutMs);
    for (size_t i = 0; i < items.size(); ++i) {
        short r = 0;
        if (fds[i].revents & POLLIN) r |= kPollIn;
        if (fds[i].revents & POLLOUT) r |= kPollOut;
        if (fds[i].revents & (POLLERR | POLLNVAL)) r |= kPollErr;
        if (fds[i].revents & POLLHUP) r |= kPollHup;
        items[i].revents = r;
    }
    return rc;
#endif
}

void shutdownSocket(socket_t fd) {
#if defined(TQ_WINDOWS)
    ::shutdown(fd, SD_BOTH);
#else
    ::shutdown(fd, SHUT_RDWR);
#endif
}

bool isLoopbackAddress(const std::string& address) {
    if (str::startsWith(address, "127.")) return true;
    if (address == "::1") return true;
    if (str::startsWith(address, "::ffff:127.")) return true;
    return address == "localhost";
}

}  // namespace tq::net
