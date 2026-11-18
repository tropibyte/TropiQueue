// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <set>
#include <string>
#include <vector>

#include "security/users.h"

namespace tq::security {

class JwtValidator;

struct AuthResult {
    bool ok = false;
    std::string user;
    std::string error;
    // True when the credentials were an access token; the caller must call
    // UserStore::dropTokenSession(user) once the session ends.
    bool token = false;
};

struct AuthenticatorOptions {
    std::vector<std::string> mechanisms = {"PLAIN", "AMQPLAIN"};
    // Users that may only connect from a loopback address (default: guest).
    std::set<std::string> loopbackUsers = {"guest"};
};

// Server side of the SASL mechanisms offered in connection.start.
class Authenticator {
public:
    Authenticator(const UserStore& users, AuthenticatorOptions options);

    std::string mechanismsString() const;
    bool supports(const std::string& mechanism) const;

    AuthResult authenticate(const std::string& mechanism, const std::string& response,
                            const std::string& peerAddress) const;

    // Used by the management API (HTTP basic auth).
    AuthResult authenticatePassword(const std::string& user, const std::string& password,
                                    const std::string& peerAddress) const;
    // Used by the management API (Authorization: Bearer).
    AuthResult authenticateToken(const std::string& token) const;

    // Enables access tokens in place of passwords. Sessions created from
    // tokens are registered in `sessions`.
    void setTokenValidator(const JwtValidator* validator, UserStore* sessions);
    bool tokensEnabled() const;

private:
    AuthResult plain(const std::string& response, const std::string& peerAddress) const;
    AuthResult amqplain(const std::string& response, const std::string& peerAddress) const;
    bool loopbackAllowed(const std::string& user, const std::string& peerAddress) const;
    bool isToken(const std::string& password) const;

    const UserStore& users_;
    AuthenticatorOptions options_;
    const JwtValidator* jwt_ = nullptr;
    UserStore* sessions_ = nullptr;
};

}  // namespace tq::security
