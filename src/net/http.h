// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "common/json.h"
#include "net/socket.h"

namespace tq::net {

struct HttpRequest {
    std::string method;
    std::string target;  // raw request target, e.g. "/api/queues/%2F?columns=name"
    std::string path;    // target without the query string (still percent-encoded)
    std::map<std::string, std::string> query;    // decoded
    std::map<std::string, std::string> headers;  // lower-cased names
    std::string body;
    std::string peerAddress;

    std::string header(const std::string& name, const std::string& def = "") const;
    std::string queryParam(const std::string& name, const std::string& def = "") const;
};

struct HttpResponse {
    int status = 200;
    std::map<std::string, std::string> headers;
    std::string body;

    static HttpResponse json(int status, const Json& body);
    static HttpResponse text(int status, std::string body, std::string contentType = "text/plain; charset=utf-8");
    static HttpResponse noContent();
    static HttpResponse error(int status, const std::string& error, const std::string& reason);
};

const char* httpReasonPhrase(int status);

using PathParams = std::map<std::string, std::string>;
using HttpHandler = std::function<HttpResponse(const HttpRequest&, const PathParams&)>;

// Matches "/api/queues/{vhost}/{name}" style patterns; captures are
// percent-decoded.
class HttpRouter {
public:
    void add(const std::string& method, const std::string& pattern, HttpHandler handler);
    // Returns nullopt when no route matches. Sets *methodMismatch when the
    // path matched but the method did not.
    std::optional<HttpResponse> dispatch(const HttpRequest& req, bool* methodMismatch = nullptr) const;

private:
    struct Route {
        std::string method;
        std::vector<std::string> segments;
        HttpHandler handler;
    };
    static bool match(const Route& r, const std::vector<std::string>& parts, PathParams& params);
    std::vector<Route> routes_;
};

// Minimal blocking HTTP/1.1 server with keep-alive.
class HttpServer {
public:
    using Handler = std::function<HttpResponse(const HttpRequest&)>;

    explicit HttpServer(Handler handler);
    ~HttpServer();

    bool start(const std::string& host, uint16_t port, std::string* error);
    void stop();
    uint16_t port() const { return port_; }

private:
    void acceptLoop();
    void serve(socket_t fd, std::string peer);
    bool readRequest(Socket& sock, std::string& buf, HttpRequest& req);

    Handler handler_;
    Socket listener_;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::thread acceptThread_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::map<uint64_t, socket_t> active_;
    uint64_t nextConn_ = 1;
};

std::map<std::string, std::string> parseQueryString(const std::string& qs);

// Simple blocking client used by tqctl and the test-suite.
struct HttpClientResponse {
    int status = 0;
    std::map<std::string, std::string> headers;
    std::string body;
    std::string error;  // transport error, empty on success
};

HttpClientResponse httpRequest(const std::string& host, uint16_t port, const std::string& method,
                               const std::string& target, const std::map<std::string, std::string>& headers = {},
                               const std::string& body = "", int timeoutMs = 10000);

}  // namespace tq::net
