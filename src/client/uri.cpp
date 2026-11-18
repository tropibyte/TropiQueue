// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "client/uri.h"

#include "common/strings.h"
#include "net/http.h"

namespace tq::client {

std::string ConnectionParams::toString() const {
    return "amqp://" + user + ":" + password + "@" + host + ":" + std::to_string(port) + "/" + str::urlEncode(vhost);
}

std::optional<ConnectionParams> parseUri(const std::string& uri, std::string* error) {
    ConnectionParams p;
    auto fail = [&](const std::string& why) -> std::optional<ConnectionParams> {
        if (error) *error = why;
        return std::nullopt;
    };
    std::string rest;
    if (str::startsWith(uri, "amqp://")) {
        rest = uri.substr(7);
    } else {
        return fail("URI must start with amqp://");
    }
    std::string query;
    size_t q = rest.find('?');
    if (q != std::string::npos) {
        query = rest.substr(q + 1);
        rest = rest.substr(0, q);
    }
    std::string path;
    size_t slash = rest.find('/');
    if (slash != std::string::npos) {
        path = rest.substr(slash + 1);
        rest = rest.substr(0, slash);
        auto vhost = str::urlDecode(path);
        if (!vhost) return fail("invalid virtual host encoding");
        p.vhost = path.empty() ? "/" : *vhost;
    }
    size_t at = rest.rfind('@');
    if (at != std::string::npos) {
        std::string userinfo = rest.substr(0, at);
        rest = rest.substr(at + 1);
        size_t colon = userinfo.find(':');
        auto u = str::urlDecode(userinfo.substr(0, colon));
        if (!u) return fail("invalid user name encoding");
        p.user = *u;
        if (colon != std::string::npos) {
            auto pw = str::urlDecode(userinfo.substr(colon + 1));
            if (!pw) return fail("invalid password encoding");
            p.password = *pw;
        }
    }
    if (!rest.empty()) {
        auto ep = net::parseEndpoint(rest, 5672);
        if (!ep) return fail("invalid host or port");
        p.host = ep->host == "0.0.0.0" ? "localhost" : ep->host;
        p.port = ep->port;
    }
    for (const auto& [k, v] : net::parseQueryString(query)) {
        if (k == "heartbeat") p.heartbeat = static_cast<uint16_t>(str::parseInt(v).value_or(p.heartbeat));
        else if (k == "frame_max") p.frameMax = static_cast<uint32_t>(str::parseInt(v).value_or(p.frameMax));
        else if (k == "channel_max") p.channelMax = static_cast<uint16_t>(str::parseInt(v).value_or(p.channelMax));
        else if (k == "connection_timeout") p.connectTimeoutMs = static_cast<int>(str::parseInt(v).value_or(5000));
        else if (k == "name") p.connectionName = v;
        else if (k == "auth_mechanism") p.mechanism = v;
    }
    return p;
}

}  // namespace tq::client
