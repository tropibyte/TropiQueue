// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "mgmt/management.h"

#include "broker/broker.h"
#include "common/crypto.h"
#include "common/logging.h"
#include "common/strings.h"
#include "mgmt/routes.h"

namespace tq::mgmt {

using net::HttpRequest;
using net::HttpResponse;

ManagementServer::ManagementServer(broker::Broker& broker) : broker_(broker) { registerRoutes(); }

ManagementServer::~ManagementServer() { stop(); }

bool ManagementServer::start(const std::string& host, uint16_t port, std::string* error) {
    server_ = std::make_unique<net::HttpServer>([this](const HttpRequest& req) { return handle(req); });
    if (!server_->start(host, port, error)) return false;
    LOG_INFO("mgmt") << "management API listening on " << host << ":" << server_->port();
    return true;
}

void ManagementServer::stop() {
    if (server_) server_->stop();
}

uint16_t ManagementServer::port() const { return server_ ? server_->port() : 0; }

void ManagementServer::registerRoutes() { Routes::registerAll(router_, broker_); }

std::optional<RequestContext> ManagementServer::authenticate(const HttpRequest& req, HttpResponse* failure) {
    std::string header = req.header("authorization");
    auto unauthorized = [&](const std::string& reason) {
        *failure = HttpResponse::error(401, "not_authorised", reason);
        failure->headers["WWW-Authenticate"] = "Basic realm=\"TropiQueue Management\"";
        return std::nullopt;
    };
    security::AuthResult auth;
    if (str::startsWith(header, "Bearer ")) {
        auth = broker_.authenticator().authenticateToken(str::trim(header.substr(7)));
    } else if (str::startsWith(header, "Basic ")) {
        auto decoded = crypto::base64Decode(str::trim(header.substr(6)));
        if (!decoded) return unauthorized("Login failed");
        size_t colon = decoded->find(':');
        if (colon == std::string::npos) return unauthorized("Login failed");
        std::string user = decoded->substr(0, colon);
        std::string pass = decoded->substr(colon + 1);
        auth = broker_.authenticator().authenticatePassword(user, pass, req.peerAddress);
    } else {
        return unauthorized("Login failed");
    }
    if (!auth.ok) return unauthorized(auth.error);
    auto u = broker_.users().getUser(auth.user);
    auto release = [&] {
        if (auth.token) broker_.users().dropTokenSession(auth.user);
    };
    if (!u) {
        release();
        return unauthorized("Login failed");
    }
    if (!u->hasTag("management") && !u->hasTag("monitoring") && !u->hasTag("policymaker") && !u->isAdministrator()) {
        release();
        *failure = HttpResponse::error(401, "not_authorised", "Not management user");
        return std::nullopt;
    }
    RequestContext ctx;
    ctx.user = *u;
    ctx.request = &req;
    ctx.viaToken = auth.token;
    return ctx;
}

void ManagementServer::addCors(const HttpRequest& req, HttpResponse& resp) const {
    const std::string& allowed = broker_.config().managementCorsOrigin;
    if (allowed.empty()) return;
    std::string origin = req.header("origin");
    if (origin.empty()) return;
    bool ok = allowed == "*";
    for (const auto& o : str::splitTrimmed(allowed, ',')) {
        if (o == origin) ok = true;
    }
    if (!ok) return;
    resp.headers["Access-Control-Allow-Origin"] = origin;
    resp.headers["Access-Control-Allow-Credentials"] = "true";
    resp.headers["Vary"] = "Origin";
}

HttpResponse ManagementServer::handle(const HttpRequest& req) {
    if (req.method == "OPTIONS") {
        HttpResponse r = HttpResponse::noContent();
        r.headers["Access-Control-Allow-Methods"] = "GET, PUT, POST, DELETE, OPTIONS";
        r.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type";
        addCors(req, r);
        return r;
    }

    std::optional<RequestContext> ctx;
    bool internal = req.path == "/metrics" || req.target.find("/api/health/") != std::string::npos;
    bool publicFile = req.method == "GET" && (req.path == "/" || str::startsWith(req.path, "/ui/"));
    if (publicFile) {
        RequestContext anonymous;
        anonymous.request = &req;
        ctx = anonymous;
    } else if (internal) {
        RequestContext system;
        system.user.name = "tropiqueue-internal";
        system.user.tags = {"administrator"};
        system.request = &req;
        ctx = system;
    } else {
        HttpResponse failure;
        ctx = authenticate(req, &failure);
        if (!ctx) {
            addCors(req, failure);
            return failure;
        }
    }

    Routes::setContext(&*ctx);
    HttpResponse resp;
    try {
        bool methodMismatch = false;
        auto routed = router_.dispatch(req, &methodMismatch);
        if (routed) {
            resp = std::move(*routed);
        } else if (methodMismatch) {
            resp = HttpResponse::error(405, "method_not_allowed", "Method Not Allowed");
        } else {
            resp = HttpResponse::error(404, "not_found", "Object Not Found");
        }
    } catch (const RouteError& e) {
        resp = HttpResponse::error(e.status, e.error, e.what());
    } catch (const JsonError& e) {
        resp = HttpResponse::error(400, "bad_request", e.what());
    }
    Routes::setContext(nullptr);
    if (ctx->viaToken) broker_.users().dropTokenSession(ctx->user.name);
    addCors(req, resp);
    return resp;
}

}  // namespace tq::mgmt
