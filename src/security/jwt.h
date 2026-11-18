// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// OAuth 2.0 access tokens (JWT, HS256) as an alternative to passwords. The
// token is sent as the password (AMQP PLAIN, MQTT, STOMP) or as a bearer
// token (management API). Scopes follow the RabbitMQ convention:
//
//   <resource-server-id>.read:<vhost>/<resource>
//   <resource-server-id>.write:<vhost>/<resource>
//   <resource-server-id>.configure:<vhost>/<resource>
//   <resource-server-id>.tag:<tag>
//
// where vhost and resource may contain '*' wildcards.
#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "common/json.h"
#include "security/users.h"

namespace tq::security {

struct JwtConfig {
    bool enabled = false;
    std::string resourceServerId = "tropiqueue";
    std::string audience;                       // defaults to resourceServerId
    std::string issuer;                         // optional
    std::string defaultKey;                     // HS256 secret used when no "kid" is given
    std::map<std::string, std::string> keys;    // kid -> HS256 secret
    std::string usernameClaim = "sub";
    int64_t leewaySeconds = 60;
};

struct TokenIdentity {
    std::string user;
    std::set<std::string> tags;
    std::vector<ScopeGrant> grants;
    int64_t expiresAt = 0;  // seconds since epoch, 0 = no expiry
    Json claims;

    TokenSession toSession() const;
};

class JwtValidator {
public:
    explicit JwtValidator(JwtConfig config);

    bool enabled() const { return config_.enabled; }
    const JwtConfig& config() const { return config_; }

    // Heuristic used to decide whether a password is a token.
    static bool looksLikeJwt(const std::string& s);

    std::optional<TokenIdentity> validate(const std::string& token, std::string* error) const;

    // Creates an HS256 token (used by tests and tqctl).
    static std::string sign(const Json& header, const Json& payload, const std::string& key);

private:
    std::vector<ScopeGrant> grantsFromScopes(const std::vector<std::string>& scopes, std::set<std::string>& tags) const;

    JwtConfig config_;
};

std::string base64UrlEncode(const std::string& data);
std::optional<std::string> base64UrlDecode(const std::string& text);

}  // namespace tq::security
