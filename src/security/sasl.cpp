// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "security/sasl.h"

#include <algorithm>

#include "amqp/field_table.h"
#include "common/bytes.h"
#include "common/logging.h"
#include "common/strings.h"
#include "net/socket.h"
#include "security/jwt.h"

namespace tq::security {

Authenticator::Authenticator(const UserStore& users, AuthenticatorOptions options)
    : users_(users), options_(std::move(options)) {}

std::string Authenticator::mechanismsString() const { return str::join(options_.mechanisms, " "); }

bool Authenticator::supports(const std::string& mechanism) const {
    return std::find(options_.mechanisms.begin(), options_.mechanisms.end(), mechanism) != options_.mechanisms.end();
}

void Authenticator::setTokenValidator(const JwtValidator* validator, UserStore* sessions) {
    jwt_ = validator;
    sessions_ = sessions;
}

bool Authenticator::tokensEnabled() const { return jwt_ && jwt_->enabled() && sessions_; }

bool Authenticator::isToken(const std::string& password) const {
    return tokensEnabled() && JwtValidator::looksLikeJwt(password);
}

AuthResult Authenticator::authenticateToken(const std::string& token) const {
    AuthResult r;
    if (!tokensEnabled()) {
        r.error = "token authentication is not enabled";
        return r;
    }
    std::string why;
    auto id = jwt_->validate(token, &why);
    if (!id) {
        LOG_DEBUG("auth") << "access token rejected: " << why;
        r.error = "ACCESS_REFUSED - " + why;
        return r;
    }
    sessions_->putTokenSession(id->toSession());
    r.ok = true;
    r.user = id->user;
    r.token = true;
    return r;
}

bool Authenticator::loopbackAllowed(const std::string& user, const std::string& peerAddress) const {
    if (!options_.loopbackUsers.count(user)) return true;
    return net::isLoopbackAddress(peerAddress);
}

AuthResult Authenticator::authenticate(const std::string& mechanism, const std::string& response,
                                       const std::string& peerAddress) const {
    if (!supports(mechanism)) {
        AuthResult r;
        r.error = "unsupported authentication mechanism '" + mechanism + "'";
        return r;
    }
    if (mechanism == "PLAIN") return plain(response, peerAddress);
    if (mechanism == "AMQPLAIN") return amqplain(response, peerAddress);
    AuthResult r;
    r.error = "mechanism '" + mechanism + "' is not implemented";
    return r;
}

// RFC 4616: [authzid] NUL authcid NUL passwd
AuthResult Authenticator::plain(const std::string& response, const std::string& peerAddress) const {
    AuthResult r;
    size_t first = response.find('\0');
    size_t second = first == std::string::npos ? std::string::npos : response.find('\0', first + 1);
    if (first == std::string::npos || second == std::string::npos) {
        r.error = "malformed PLAIN response";
        return r;
    }
    std::string authzid = response.substr(0, first);
    std::string authcid = response.substr(first + 1, second - first - 1);
    std::string password = response.substr(second + 1);

    if (isToken(password)) return authenticateToken(password);

    std::string identity = authzid.empty() ? authcid : authzid;
    if (!loopbackAllowed(identity, peerAddress)) {
        r.error = "user '" + identity + "' can only connect via localhost";
        return r;
    }
    if (!users_.authenticate(authcid, password)) {
        r.error = "ACCESS_REFUSED - Login was refused using authentication mechanism PLAIN";
        return r;
    }
    r.ok = true;
    r.user = identity;
    return r;
}

// AMQPLAIN: a field table body (without its length prefix) holding LOGIN and PASSWORD.
AuthResult Authenticator::amqplain(const std::string& response, const std::string& peerAddress) const {
    (void)peerAddress;
    AuthResult r;
    amqp::FieldTable table;
    try {
        ByteWriter w;
        w.u32(static_cast<uint32_t>(response.size()));
        w.bytes(response);
        ByteReader reader(w.data());
        table = amqp::decodeFieldTable(reader);
    } catch (const DecodeError& e) {
        r.error = std::string("malformed AMQPLAIN response: ") + e.what();
        return r;
    }
    auto login = table.getString("LOGIN");
    auto password = table.getString("PASSWORD");
    if (!login || !password) {
        r.error = "AMQPLAIN response must contain LOGIN and PASSWORD";
        return r;
    }
    if (isToken(*password)) return authenticateToken(*password);
    if (!users_.authenticate(*login, *password)) {
        r.error = "ACCESS_REFUSED - Login was refused using authentication mechanism AMQPLAIN";
        return r;
    }
    r.ok = true;
    r.user = *login;
    return r;
}

AuthResult Authenticator::authenticatePassword(const std::string& user, const std::string& password,
                                               const std::string& peerAddress) const {
    if (isToken(password)) return authenticateToken(password);
    AuthResult r;
    if (!loopbackAllowed(user, peerAddress)) {
        r.error = "user '" + user + "' can only connect via localhost";
        return r;
    }
    if (!users_.authenticate(user, password)) {
        r.error = "Not authorised";
        return r;
    }
    r.ok = true;
    r.user = user;
    return r;
}

}  // namespace tq::security
