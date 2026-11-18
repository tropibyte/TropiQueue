// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <stdexcept>
#include <string>

#include "mgmt/management.h"
#include "net/http.h"

namespace tq::broker {
class Broker;
}

namespace tq::mgmt {

class RouteError : public std::runtime_error {
public:
    RouteError(int status, std::string error, const std::string& reason)
        : std::runtime_error(reason), status(status), error(std::move(error)) {}
    int status;
    std::string error;
};

// Registers every management API endpoint on the router.
class Routes {
public:
    static void registerAll(net::HttpRouter& router, broker::Broker& broker);
    // Runtime parameters, shovels, federation, user limits, tracing and the
    // web UI (routes_ext.cpp).
    static void registerExtended(net::HttpRouter& router, broker::Broker& broker);
    static void setContext(RequestContext* ctx);
    static RequestContext& context();
};

// Prometheus text exposition of broker, queue and connection metrics.
std::string renderMetrics(broker::Broker& broker);

}  // namespace tq::mgmt
