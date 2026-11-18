// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "net/http.h"

#include "common/logging.h"
#include "common/strings.h"

namespace tq::net {

std::string HttpRequest::header(const std::string& name, const std::string& def) const {
    auto it = headers.find(str::toLower(name));
    return it == headers.end() ? def : it->second;
}

std::string HttpRequest::queryParam(const std::string& name, const std::string& def) const {
    auto it = query.find(name);
    return it == query.end() ? def : it->second;
}

const char* httpReasonPhrase(int status) {
    switch (status) {
        case 200: return "OK";
        case 201: return "Created";
        case 204: return "No Content";
        case 301: return "Moved Permanently";
        case 400: return "Bad Request";
        case 401: return "Unauthorized";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 409: return "Conflict";
        case 413: return "Payload Too Large";
        case 415: return "Unsupported Media Type";
        case 429: return "Too Many Requests";
        case 500: return "Internal Server Error";
        case 503: return "Service Unavailable";
        default: return "Unknown";
    }
}

HttpResponse HttpResponse::json(int status, const Json& body) {
    HttpResponse r;
    r.status = status;
    r.headers["Content-Type"] = "application/json";
    r.body = body.dump();
    return r;
}

HttpResponse HttpResponse::text(int status, std::string body, std::string contentType) {
    HttpResponse r;
    r.status = status;
    r.headers["Content-Type"] = std::move(contentType);
    r.body = std::move(body);
    return r;
}

HttpResponse HttpResponse::noContent() {
    HttpResponse r;
    r.status = 204;
    return r;
}

HttpResponse HttpResponse::error(int status, const std::string& error, const std::string& reason) {
    Json j = Json::object();
    j["error"] = error;
    j["reason"] = reason;
    return json(status, j);
}

std::map<std::string, std::string> parseQueryString(const std::string& qs) {
    std::map<std::string, std::string> out;
    for (const auto& pair : str::split(qs, '&')) {
        if (pair.empty()) continue;
        size_t eq = pair.find('=');
        std::string k = pair.substr(0, eq);
        std::string v = eq == std::string::npos ? "" : pair.substr(eq + 1);
        auto dk = str::urlDecode(k), dv = str::urlDecode(v);
        if (dk && dv) out[*dk] = *dv;
    }
    return out;
}

// ----------------------------------------------------------------- HttpRouter

void HttpRouter::add(const std::string& method, const std::string& pattern, HttpHandler handler) {
    Route r;
    r.method = method;
    for (auto& s : str::split(pattern, '/')) {
        if (!s.empty()) r.segments.push_back(s);
    }
    r.handler = std::move(handler);
    routes_.push_back(std::move(r));
}

bool HttpRouter::match(const Route& r, const std::vector<std::string>& parts, PathParams& params) {
    if (r.segments.size() != parts.size()) return false;
    for (size_t i = 0; i < parts.size(); ++i) {
        const auto& seg = r.segments[i];
        if (seg.size() >= 2 && seg.front() == '{' && seg.back() == '}') {
            auto decoded = str::urlDecode(parts[i]);
            if (!decoded) return false;
            params[seg.substr(1, seg.size() - 2)] = *decoded;
        } else if (seg != parts[i]) {
            return false;
        }
    }
    return true;
}

std::optional<HttpResponse> HttpRouter::dispatch(const HttpRequest& req, bool* methodMismatch) const {
    std::vector<std::string> parts;
    for (auto& s : str::split(req.path, '/')) {
        if (!s.empty()) parts.push_back(s);
    }
    bool mismatch = false;
    for (const auto& r : routes_) {
        PathParams params;
        if (!match(r, parts, params)) continue;
        if (r.method != req.method) {
            mismatch = true;
            continue;
        }
        return r.handler(req, params);
    }
    if (methodMismatch) *methodMismatch = mismatch;
    return std::nullopt;
}

// ----------------------------------------------------------------- HttpServer

HttpServer::HttpServer(Handler handler) : handler_(std::move(handler)) {}

HttpServer::~HttpServer() { stop(); }

bool HttpServer::start(const std::string& host, uint16_t port, std::string* error) {
    listener_ = Socket::listenTcp(host, port, 64, error);
    if (!listener_.valid()) return false;
    port_ = listener_.localPort();
    running_.store(true);
    acceptThread_ = std::thread([this] { acceptLoop(); });
    return true;
}

void HttpServer::stop() {
    if (!running_.exchange(false)) return;
    if (acceptThread_.joinable()) acceptThread_.join();
    listener_.close();
    std::unique_lock<std::mutex> lock(mu_);
    for (auto& [id, fd] : active_) {
        shutdownSocket(fd);
    }
    cv_.wait_for(lock, std::chrono::seconds(5), [this] { return active_.empty(); });
}

void HttpServer::acceptLoop() {
    while (running_.load()) {
        std::vector<PollItem> items{{listener_.fd(), kPollIn, 0}};
        if (pollSockets(items, 200) <= 0) continue;
        std::string peer;
        uint16_t peerPort = 0;
        Socket s = listener_.accept(&peer, &peerPort);
        if (!s.valid()) continue;
        socket_t fd = s.release();
        std::thread([this, fd, peer] { serve(fd, peer); }).detach();
    }
}

bool HttpServer::readRequest(Socket& sock, std::string& buf, HttpRequest& req) {
    size_t headerEnd;
    char tmp[8192];
    while ((headerEnd = buf.find("\r\n\r\n")) == std::string::npos) {
        long n = sock.recv(tmp, sizeof tmp);
        if (n <= 0) return false;
        buf.append(tmp, static_cast<size_t>(n));
    }
    std::string head = buf.substr(0, headerEnd);
    buf.erase(0, headerEnd + 4);

    auto lines = str::split(head, '\n');
    if (lines.empty()) return false;
    auto requestLine = str::split(str::trim(lines[0]), ' ');
    if (requestLine.size() < 2) return false;
    req.method = requestLine[0];
    req.target = requestLine[1];
    size_t q = req.target.find('?');
    req.path = req.target.substr(0, q);
    req.query = q == std::string::npos ? std::map<std::string, std::string>{} : parseQueryString(req.target.substr(q + 1));
    for (size_t i = 1; i < lines.size(); ++i) {
        std::string line = str::trim(lines[i]);
        size_t colon = line.find(':');
        if (colon == std::string::npos) continue;
        req.headers[str::toLower(str::trim(line.substr(0, colon)))] = str::trim(line.substr(colon + 1));
    }

    size_t contentLength = 0;
    auto cl = req.headers.find("content-length");
    if (cl != req.headers.end()) {
        auto v = str::parseUint(cl->second);
        if (!v) return false;
        contentLength = static_cast<size_t>(*v);
    }
    while (buf.size() < contentLength) {
        long n = sock.recv(tmp, sizeof tmp);
        if (n <= 0) return false;
        buf.append(tmp, static_cast<size_t>(n));
    }
    req.body = buf.substr(0, contentLength);
    buf.clear();
    return true;
}

void HttpServer::serve(socket_t fd, std::string peer) {
    uint64_t connId;
    {
        std::lock_guard<std::mutex> lock(mu_);
        connId = nextConn_++;
        active_[connId] = fd;
    }
    Socket sock(fd);
    sock.setReceiveTimeout(30000);
    std::string buf;
    while (running_.load()) {
        HttpRequest req;
        req.peerAddress = peer;
        if (!readRequest(sock, buf, req)) break;
        HttpResponse resp;
        try {
            resp = handler_(req);
        } catch (const std::exception& e) {
            LOG_ERROR("http") << req.method << " " << req.path << " failed: " << e.what();
            resp = HttpResponse::error(500, "internal_error", e.what());
        }
        bool keepAlive = !str::iequals(req.header("connection"), "close");
        std::string out = "HTTP/1.1 " + std::to_string(resp.status) + " " + httpReasonPhrase(resp.status) + "\r\n";
        resp.headers["Content-Length"] = std::to_string(resp.body.size());
        resp.headers["Server"] = "TropiQueue";
        if (!keepAlive) resp.headers["Connection"] = "close";
        for (const auto& [k, v] : resp.headers) out += k + ": " + v + "\r\n";
        out += "\r\n";
        if (req.method != "HEAD") out += resp.body;
        if (!sock.sendAll(out.data(), out.size())) break;
        if (!keepAlive) break;
    }
    sock.close();
    {
        std::lock_guard<std::mutex> lock(mu_);
        active_.erase(connId);
    }
    cv_.notify_all();
}

// ----------------------------------------------------------------- HttpClient

HttpClientResponse httpRequest(const std::string& host, uint16_t port, const std::string& method,
                               const std::string& target, const std::map<std::string, std::string>& headers,
                               const std::string& body, int timeoutMs) {
    HttpClientResponse resp;
    std::string err;
    Socket sock = Socket::connectTcp(host, port, timeoutMs, &err);
    if (!sock.valid()) {
        resp.error = err;
        return resp;
    }
    sock.setReceiveTimeout(timeoutMs);
    std::string req = method + " " + target + " HTTP/1.1\r\nHost: " + host + ":" + std::to_string(port) +
                      "\r\nConnection: close\r\n";
    for (const auto& [k, v] : headers) req += k + ": " + v + "\r\n";
    req += "Content-Length: " + std::to_string(body.size()) + "\r\n\r\n" + body;
    if (!sock.sendAll(req.data(), req.size())) {
        resp.error = "send failed";
        return resp;
    }
    std::string buf;
    char tmp[8192];
    while (true) {
        long n = sock.recv(tmp, sizeof tmp);
        if (n <= 0) break;
        buf.append(tmp, static_cast<size_t>(n));
    }
    size_t headerEnd = buf.find("\r\n\r\n");
    if (headerEnd == std::string::npos) {
        resp.error = "malformed response";
        return resp;
    }
    auto lines = str::split(buf.substr(0, headerEnd), '\n');
    auto statusLine = str::split(str::trim(lines[0]), ' ');
    if (statusLine.size() >= 2) resp.status = static_cast<int>(str::parseInt(statusLine[1]).value_or(0));
    for (size_t i = 1; i < lines.size(); ++i) {
        std::string line = str::trim(lines[i]);
        size_t colon = line.find(':');
        if (colon == std::string::npos) continue;
        resp.headers[str::toLower(str::trim(line.substr(0, colon)))] = str::trim(line.substr(colon + 1));
    }
    resp.body = buf.substr(headerEnd + 4);
    return resp;
}

}  // namespace tq::net
