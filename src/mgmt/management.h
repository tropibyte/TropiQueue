// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <memory>
#include <optional>
#include <string>

#include "net/http.h"
#include "security/users.h"

namespace tq::broker {
class Broker;
}

namespace tq::mgmt {

// Authenticated management user for one request.
struct RequestContext {
    security::User user;
    const net::HttpRequest* request = nullptr;
    bool viaToken = false;
    bool isAdmin() const { return user.isAdministrator(); }
    bool hasTag(const std::string& t) const { return user.hasTag(t) || isAdmin(); }
};

// RabbitMQ-compatible management HTTP API plus Prometheus metrics.
class ManagementServer {
public:
    explicit ManagementServer(broker::Broker& broker);
    ~ManagementServer();

    bool start(const std::string& host, uint16_t port, std::string* error);
    void stop();
    uint16_t port() const;

    // Exposed for tests: runs one request through authentication and routing.
    net::HttpResponse handle(const net::HttpRequest& req);

private:
    void registerRoutes();
    std::optional<RequestContext> authenticate(const net::HttpRequest& req, net::HttpResponse* failure);
    void addCors(const net::HttpRequest& req, net::HttpResponse& resp) const;

    broker::Broker& broker_;
    net::HttpRouter router_;
    std::unique_ptr<net::HttpServer> server_;
};

}  // namespace tq::mgmt
