// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "security/jwt.h"

#include "common/clock.h"
#include "common/crypto.h"
#include "common/strings.h"

namespace tq::security {

std::string base64UrlEncode(const std::string& data) {
    std::string s = crypto::base64Encode(data);
    for (auto& c : s) {
        if (c == '+') c = '-';
        else if (c == '/') c = '_';
    }
    while (!s.empty() && s.back() == '=') s.pop_back();
    return s;
}

std::optional<std::string> base64UrlDecode(const std::string& text) {
    std::string s = text;
    for (auto& c : s) {
        if (c == '-') c = '+';
        else if (c == '_') c = '/';
    }
    while (s.size() % 4 != 0) s.push_back('=');
    return crypto::base64Decode(s);
}

TokenSession TokenIdentity::toSession() const {
    TokenSession s;
    s.user = user;
    s.tags = tags;
    s.grants = grants;
    s.expiresAt = expiresAt;
    return s;
}

JwtValidator::JwtValidator(JwtConfig config) : config_(std::move(config)) {
    if (config_.audience.empty()) config_.audience = config_.resourceServerId;
}

bool JwtValidator::looksLikeJwt(const std::string& s) {
    if (s.size() < 16) return false;
    size_t first = s.find('.');
    if (first == std::string::npos) return false;
    size_t second = s.find('.', first + 1);
    if (second == std::string::npos || s.find('.', second + 1) != std::string::npos) return false;
    return str::startsWith(s, "eyJ");
}

std::string JwtValidator::sign(const Json& header, const Json& payload, const std::string& key) {
    std::string signingInput = base64UrlEncode(header.dump()) + "." + base64UrlEncode(payload.dump());
    std::string alg = header["alg"].asString("HS256");
    std::string sig = alg == "HS256" ? base64UrlEncode(crypto::hmacSha256(key, signingInput)) : "";
    return signingInput + "." + sig;
}

std::vector<ScopeGrant> JwtValidator::grantsFromScopes(const std::vector<std::string>& scopes,
                                                       std::set<std::string>& tags) const {
    std::vector<ScopeGrant> grants;
    std::string prefix = config_.resourceServerId + ".";
    for (const auto& scope : scopes) {
        if (!str::startsWith(scope, prefix)) continue;
        std::string rest = scope.substr(prefix.size());
        size_t colon = rest.find(':');
        if (colon == std::string::npos) continue;
        std::string kind = rest.substr(0, colon);
        std::string target = rest.substr(colon + 1);
        if (kind == "tag") {
            tags.insert(target);
            continue;
        }
        ScopeGrant g;
        if (kind == "read") g.access = Access::Read;
        else if (kind == "write") g.access = Access::Write;
        else if (kind == "configure") g.access = Access::Configure;
        else continue;
        size_t slash = target.find('/');
        g.vhostPattern = slash == std::string::npos ? target : target.substr(0, slash);
        g.resourcePattern = slash == std::string::npos ? "*" : target.substr(slash + 1);
        grants.push_back(g);
    }
    return grants;
}

std::optional<TokenIdentity> JwtValidator::validate(const std::string& token, std::string* error) const {
    auto fail = [&](const std::string& why) -> std::optional<TokenIdentity> {
        if (error) *error = why;
        return std::nullopt;
    };
    if (!config_.enabled) return fail("token authentication is disabled");
    auto parts = str::split(token, '.');
    if (parts.size() != 3) return fail("malformed token");
    auto headerRaw = base64UrlDecode(parts[0]);
    auto payloadRaw = base64UrlDecode(parts[1]);
    if (!headerRaw || !payloadRaw) return fail("malformed token encoding");
    Json header, payload;
    try {
        header = Json::parse(*headerRaw);
        payload = Json::parse(*payloadRaw);
    } catch (const JsonError&) {
        return fail("malformed token JSON");
    }
    std::string alg = header["alg"].asString();
    if (alg != "HS256" && alg != "none") return fail("unsupported token algorithm '" + alg + "'");
    if (alg == "HS256") {
        std::string key = config_.defaultKey;
        if (auto kid = header.find("kid")) {
            auto it = config_.keys.find(kid->asString());
            if (it == config_.keys.end()) return fail("unknown signing key '" + kid->asString() + "'");
            key = it->second;
        }
        if (key.empty()) return fail("no signing key configured");
        std::string expected = crypto::hmacSha256(key, parts[0] + "." + parts[1]);
        auto actual = base64UrlDecode(parts[2]);
        if (!actual || !crypto::constantTimeEquals(*actual, expected)) return fail("invalid token signature");
    }

    int64_t now = wallClockMs() / 1000;
    if (auto exp = payload.find("exp")) {
        if (exp->asInt() + config_.leewaySeconds < now) return fail("token has expired");
    }
    if (auto nbf = payload.find("nbf")) {
        if (nbf->asInt() - config_.leewaySeconds > now) return fail("token is not valid yet");
    }
    if (!config_.issuer.empty() && payload["iss"].asString() != config_.issuer) return fail("unexpected token issuer");
    if (auto aud = payload.find("aud")) {
        bool ok = false;
        if (aud->isString()) ok = aud->asString() == config_.audience;
        for (const auto& a : aud->asArray()) ok = ok || a.asString() == config_.audience;
        if (!ok) return fail("token audience does not include '" + config_.audience + "'");
    }

    TokenIdentity id;
    id.user = payload[config_.usernameClaim].asString();
    if (id.user.empty()) return fail("token has no '" + config_.usernameClaim + "' claim");
    std::vector<std::string> scopes;
    const Json& scope = payload["scope"];
    if (scope.isString()) scopes = str::splitTrimmed(scope.asString(), ' ');
    for (const auto& s : scope.asArray()) scopes.push_back(s.asString());
    id.grants = grantsFromScopes(scopes, id.tags);
    id.expiresAt = payload["exp"].asInt(0);
    id.claims = payload;
    return id;
}

}  // namespace tq::security
