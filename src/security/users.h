// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <shared_mutex>
#include <tuple>
#include <string>
#include <vector>

#include "common/json.h"

namespace tq::security {

constexpr const char* kHashSha256 = "tq_password_hashing_sha256";
// Accepted on import for compatibility with RabbitMQ definition files.
constexpr const char* kHashRabbitSha256 = "rabbit_password_hashing_sha256";

enum class Access { Configure, Write, Read };

// Per-user resource limits (-1 = unlimited).
struct UserLimits {
    int64_t maxConnections = -1;
    int64_t maxChannels = -1;

    bool empty() const { return maxConnections < 0 && maxChannels < 0; }
    Json toJson() const;
    static UserLimits fromJson(const Json& j);
};

struct User {
    std::string name;
    std::string passwordHash;  // base64(salt[4] || sha256(salt || password))
    std::string hashingAlgorithm = kHashSha256;
    std::set<std::string> tags;  // administrator, monitoring, management, policymaker, impersonator
    UserLimits limits;

    bool hasTag(const std::string& t) const { return tags.count(t) != 0; }
    bool isAdministrator() const { return hasTag("administrator"); }
    Json toJson(bool includeHash) const;
};

struct Permission {
    std::string user;
    std::string vhost;
    std::string configure = ".*";
    std::string write = ".*";
    std::string read = ".*";
    Json toJson() const;
};

struct TopicPermission {
    std::string user;
    std::string vhost;
    std::string exchange;
    std::string write = ".*";
    std::string read = ".*";
    Json toJson() const;
};

// A permission granted by an OAuth 2.0 scope. Patterns use '*' wildcards.
struct ScopeGrant {
    Access access = Access::Read;
    std::string vhostPattern;
    std::string resourcePattern;
};

// Identity established from an access token rather than a stored password.
struct TokenSession {
    std::string user;
    std::set<std::string> tags;
    std::vector<ScopeGrant> grants;
    int64_t expiresAt = 0;  // seconds since epoch, 0 = no expiry
};

// Users, their tags and per-vhost permissions. Permissions are regular
// expressions matched against resource names (queues and exchanges).
class UserStore {
public:
    static std::string hashPassword(const std::string& password);
    static bool checkPassword(const std::string& storedHash, const std::string& password);

    bool addUser(const std::string& name, const std::string& password, const std::set<std::string>& tags);
    void putUser(const User& user);  // replaces, keeps the given hash
    bool deleteUser(const std::string& name);
    bool changePassword(const std::string& name, const std::string& password);
    bool setTags(const std::string& name, const std::set<std::string>& tags);
    bool setLimits(const std::string& name, const UserLimits& limits);
    UserLimits limits(const std::string& name) const;
    std::optional<User> getUser(const std::string& name) const;
    std::vector<User> listUsers() const;
    // Returns the user on success.
    std::optional<User> authenticate(const std::string& name, const std::string& password) const;

    void setPermission(const Permission& p);
    bool clearPermission(const std::string& user, const std::string& vhost);
    std::optional<Permission> getPermission(const std::string& user, const std::string& vhost) const;
    std::vector<Permission> listPermissions(const std::string& vhost = "", const std::string& user = "") const;
    void clearVhost(const std::string& vhost);

    void setTopicPermission(const TopicPermission& p);
    bool clearTopicPermission(const std::string& user, const std::string& vhost, const std::string& exchange);
    std::vector<TopicPermission> listTopicPermissions(const std::string& vhost = "") const;

    // True when the user may access the vhost at all.
    bool canAccessVhost(const std::string& user, const std::string& vhost) const;
    bool checkResource(const std::string& user, const std::string& vhost, Access access,
                       const std::string& resource) const;
    // Topic authorisation for publishes to / bindings on topic exchanges.
    bool checkTopic(const std::string& user, const std::string& vhost, const std::string& exchange,
                    const std::string& routingKey, bool write) const;

    // Token-based identities live alongside stored users for as long as
    // a connection authenticated with the token is open.
    void putTokenSession(const TokenSession& session);
    void dropTokenSession(const std::string& user);
    std::optional<TokenSession> tokenSession(const std::string& user) const;
    size_t tokenSessionCount() const;

    Json toJson() const;
    void loadJson(const Json& j);
    size_t userCount() const;

private:
    mutable std::shared_mutex mu_;
    std::map<std::string, User> users_;
    std::map<std::pair<std::string, std::string>, Permission> permissions_;  // (user, vhost)
    std::map<std::tuple<std::string, std::string, std::string>, TopicPermission> topicPermissions_;
    std::map<std::string, TokenSession> tokenSessions_;
    std::map<std::string, int> tokenSessionRefs_;
};

bool regexMatches(const std::string& pattern, const std::string& value);
// Shell-style matching where '*' matches any run of characters.
bool wildcardMatch(const std::string& pattern, const std::string& value);

}  // namespace tq::security
