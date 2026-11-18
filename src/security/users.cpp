// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "security/users.h"

#include <mutex>
#include <regex>

#include "common/crypto.h"
#include "common/logging.h"
#include "common/random.h"
#include "common/strings.h"

namespace tq::security {

Json UserLimits::toJson() const {
    Json j = Json::object();
    if (maxConnections >= 0) j["max-connections"] = maxConnections;
    if (maxChannels >= 0) j["max-channels"] = maxChannels;
    return j;
}

UserLimits UserLimits::fromJson(const Json& j) {
    UserLimits l;
    l.maxConnections = j["max-connections"].asInt(-1);
    l.maxChannels = j["max-channels"].asInt(-1);
    return l;
}

Json User::toJson(bool includeHash) const {
    Json j = Json::object();
    j["name"] = name;
    Json t = Json::array();
    for (const auto& tag : tags) t.push(tag);
    j["tags"] = t;
    if (includeHash) {
        j["password_hash"] = passwordHash;
        j["hashing_algorithm"] = hashingAlgorithm;
    }
    if (!limits.empty()) j["limits"] = limits.toJson();
    return j;
}

Json Permission::toJson() const {
    Json j = Json::object();
    j["user"] = user;
    j["vhost"] = vhost;
    j["configure"] = configure;
    j["write"] = write;
    j["read"] = read;
    return j;
}

Json TopicPermission::toJson() const {
    Json j = Json::object();
    j["user"] = user;
    j["vhost"] = vhost;
    j["exchange"] = exchange;
    j["write"] = write;
    j["read"] = read;
    return j;
}

bool regexMatches(const std::string& pattern, const std::string& value) {
    try {
        std::regex re(pattern, std::regex::ECMAScript);
        return std::regex_search(value, re);
    } catch (const std::regex_error& e) {
        LOG_WARN("auth") << "invalid permission pattern '" << pattern << "': " << e.what();
        return false;
    }
}

bool wildcardMatch(const std::string& pattern, const std::string& value) {
    size_t p = 0, v = 0, star = std::string::npos, mark = 0;
    while (v < value.size()) {
        if (p < pattern.size() && pattern[p] == value[v]) {
            ++p;
            ++v;
        } else if (p < pattern.size() && pattern[p] == '*') {
            star = p++;
            mark = v;
        } else if (star != std::string::npos) {
            p = star + 1;
            v = ++mark;
        } else {
            return false;
        }
    }
    while (p < pattern.size() && pattern[p] == '*') ++p;
    return p == pattern.size();
}

// --------------------------------------------------------------- passwords

std::string UserStore::hashPassword(const std::string& password) {
    std::string salt = rnd::secureBytes(4);
    std::string digest = crypto::sha256(salt + password);
    return crypto::base64Encode(salt + digest);
}

bool UserStore::checkPassword(const std::string& storedHash, const std::string& password) {
    auto raw = crypto::base64Decode(storedHash);
    if (!raw || raw->size() != 36) return false;
    std::string salt = raw->substr(0, 4);
    std::string expected = raw->substr(4);
    return crypto::constantTimeEquals(crypto::sha256(salt + password), expected);
}

// ------------------------------------------------------------------- users

bool UserStore::addUser(const std::string& name, const std::string& password, const std::set<std::string>& tags) {
    std::unique_lock lock(mu_);
    if (users_.count(name)) return false;
    User u;
    u.name = name;
    u.passwordHash = hashPassword(password);
    u.tags = tags;
    users_[name] = std::move(u);
    return true;
}

void UserStore::putUser(const User& user) {
    std::unique_lock lock(mu_);
    auto it = users_.find(user.name);
    if (it != users_.end() && user.limits.empty()) {
        UserLimits keep = it->second.limits;
        it->second = user;
        it->second.limits = keep;
        return;
    }
    users_[user.name] = user;
}

bool UserStore::setLimits(const std::string& name, const UserLimits& limits) {
    std::unique_lock lock(mu_);
    auto it = users_.find(name);
    if (it == users_.end()) return false;
    it->second.limits = limits;
    return true;
}

UserLimits UserStore::limits(const std::string& name) const {
    std::shared_lock lock(mu_);
    auto it = users_.find(name);
    return it == users_.end() ? UserLimits{} : it->second.limits;
}

bool UserStore::deleteUser(const std::string& name) {
    std::unique_lock lock(mu_);
    if (!users_.erase(name)) return false;
    for (auto it = permissions_.begin(); it != permissions_.end();) {
        if (it->first.first == name) it = permissions_.erase(it);
        else ++it;
    }
    for (auto it = topicPermissions_.begin(); it != topicPermissions_.end();) {
        if (std::get<0>(it->first) == name) it = topicPermissions_.erase(it);
        else ++it;
    }
    return true;
}

bool UserStore::changePassword(const std::string& name, const std::string& password) {
    std::unique_lock lock(mu_);
    auto it = users_.find(name);
    if (it == users_.end()) return false;
    it->second.passwordHash = hashPassword(password);
    it->second.hashingAlgorithm = kHashSha256;
    return true;
}

bool UserStore::setTags(const std::string& name, const std::set<std::string>& tags) {
    std::unique_lock lock(mu_);
    auto it = users_.find(name);
    if (it == users_.end()) return false;
    it->second.tags = tags;
    return true;
}

std::optional<User> UserStore::getUser(const std::string& name) const {
    std::shared_lock lock(mu_);
    auto it = users_.find(name);
    if (it != users_.end()) return it->second;
    auto ts = tokenSessions_.find(name);
    if (ts == tokenSessions_.end()) return std::nullopt;
    User u;
    u.name = name;
    u.tags = ts->second.tags;
    u.hashingAlgorithm.clear();
    return u;
}

std::vector<User> UserStore::listUsers() const {
    std::shared_lock lock(mu_);
    std::vector<User> out;
    for (const auto& [n, u] : users_) out.push_back(u);
    return out;
}

std::optional<User> UserStore::authenticate(const std::string& name, const std::string& password) const {
    std::shared_lock lock(mu_);
    auto it = users_.find(name);
    if (it == users_.end()) return std::nullopt;
    if (it->second.passwordHash.empty()) return std::nullopt;
    if (!checkPassword(it->second.passwordHash, password)) return std::nullopt;
    return it->second;
}

size_t UserStore::userCount() const {
    std::shared_lock lock(mu_);
    return users_.size();
}

// ------------------------------------------------------------- permissions

void UserStore::setPermission(const Permission& p) {
    std::unique_lock lock(mu_);
    permissions_[{p.user, p.vhost}] = p;
}

bool UserStore::clearPermission(const std::string& user, const std::string& vhost) {
    std::unique_lock lock(mu_);
    return permissions_.erase({user, vhost}) > 0;
}

std::optional<Permission> UserStore::getPermission(const std::string& user, const std::string& vhost) const {
    std::shared_lock lock(mu_);
    auto it = permissions_.find({user, vhost});
    if (it == permissions_.end()) return std::nullopt;
    return it->second;
}

std::vector<Permission> UserStore::listPermissions(const std::string& vhost, const std::string& user) const {
    std::shared_lock lock(mu_);
    std::vector<Permission> out;
    for (const auto& [key, p] : permissions_) {
        if (!vhost.empty() && key.second != vhost) continue;
        if (!user.empty() && key.first != user) continue;
        out.push_back(p);
    }
    return out;
}

void UserStore::clearVhost(const std::string& vhost) {
    std::unique_lock lock(mu_);
    for (auto it = permissions_.begin(); it != permissions_.end();) {
        if (it->first.second == vhost) it = permissions_.erase(it);
        else ++it;
    }
    for (auto it = topicPermissions_.begin(); it != topicPermissions_.end();) {
        if (std::get<1>(it->first) == vhost) it = topicPermissions_.erase(it);
        else ++it;
    }
}

void UserStore::setTopicPermission(const TopicPermission& p) {
    std::unique_lock lock(mu_);
    topicPermissions_[{p.user, p.vhost, p.exchange}] = p;
}

bool UserStore::clearTopicPermission(const std::string& user, const std::string& vhost, const std::string& exchange) {
    std::unique_lock lock(mu_);
    return topicPermissions_.erase({user, vhost, exchange}) > 0;
}

std::vector<TopicPermission> UserStore::listTopicPermissions(const std::string& vhost) const {
    std::shared_lock lock(mu_);
    std::vector<TopicPermission> out;
    for (const auto& [key, p] : topicPermissions_) {
        if (vhost.empty() || std::get<1>(key) == vhost) out.push_back(p);
    }
    return out;
}

// ----------------------------------------------------------- token sessions

void UserStore::putTokenSession(const TokenSession& session) {
    std::unique_lock lock(mu_);
    tokenSessions_[session.user] = session;
    ++tokenSessionRefs_[session.user];
}

void UserStore::dropTokenSession(const std::string& user) {
    std::unique_lock lock(mu_);
    auto it = tokenSessionRefs_.find(user);
    if (it == tokenSessionRefs_.end()) return;
    if (--it->second > 0) return;
    tokenSessionRefs_.erase(it);
    tokenSessions_.erase(user);
}

std::optional<TokenSession> UserStore::tokenSession(const std::string& user) const {
    std::shared_lock lock(mu_);
    auto it = tokenSessions_.find(user);
    if (it == tokenSessions_.end()) return std::nullopt;
    return it->second;
}

size_t UserStore::tokenSessionCount() const {
    std::shared_lock lock(mu_);
    return tokenSessions_.size();
}

bool UserStore::canAccessVhost(const std::string& user, const std::string& vhost) const {
    std::shared_lock lock(mu_);
    auto ts = tokenSessions_.find(user);
    if (ts != tokenSessions_.end()) {
        for (const auto& g : ts->second.grants) {
            if (wildcardMatch(g.vhostPattern, vhost)) return true;
        }
        return false;
    }
    return permissions_.count({user, vhost}) != 0;
}

bool UserStore::checkResource(const std::string& user, const std::string& vhost, Access access,
                              const std::string& resource) const {
    std::string pattern;
    {
        std::shared_lock lock(mu_);
        auto ts = tokenSessions_.find(user);
        if (ts != tokenSessions_.end()) {
            for (const auto& g : ts->second.grants) {
                if (g.access == access && wildcardMatch(g.vhostPattern, vhost) &&
                    wildcardMatch(g.resourcePattern, resource)) {
                    return true;
                }
            }
            return false;
        }
        auto it = permissions_.find({user, vhost});
        if (it == permissions_.end()) return false;
        switch (access) {
            case Access::Configure: pattern = it->second.configure; break;
            case Access::Write: pattern = it->second.write; break;
            case Access::Read: pattern = it->second.read; break;
        }
    }
    return regexMatches(pattern, resource);
}

bool UserStore::checkTopic(const std::string& user, const std::string& vhost, const std::string& exchange,
                           const std::string& routingKey, bool write) const {
    std::string pattern;
    {
        std::shared_lock lock(mu_);
        auto it = topicPermissions_.find({user, vhost, exchange});
        if (it == topicPermissions_.end()) return true;  // no topic permission = no restriction
        pattern = write ? it->second.write : it->second.read;
    }
    pattern = str::replaceAll(pattern, "{username}", user);
    pattern = str::replaceAll(pattern, "{vhost}", vhost);
    return regexMatches(pattern, routingKey);
}

// ------------------------------------------------------------- persistence

Json UserStore::toJson() const {
    std::shared_lock lock(mu_);
    Json j = Json::object();
    Json users = Json::array();
    for (const auto& [n, u] : users_) users.push(u.toJson(true));
    j["users"] = users;
    Json perms = Json::array();
    for (const auto& [k, p] : permissions_) perms.push(p.toJson());
    j["permissions"] = perms;
    Json topics = Json::array();
    for (const auto& [k, p] : topicPermissions_) topics.push(p.toJson());
    j["topic_permissions"] = topics;
    return j;
}

void UserStore::loadJson(const Json& j) {
    std::unique_lock lock(mu_);
    users_.clear();
    permissions_.clear();
    topicPermissions_.clear();
    for (const auto& uj : j["users"].asArray()) {
        User u;
        u.name = uj["name"].asString();
        u.passwordHash = uj["password_hash"].asString();
        u.hashingAlgorithm = uj["hashing_algorithm"].asString(kHashSha256);
        if (uj["tags"].isArray()) {
            for (const auto& t : uj["tags"].asArray()) u.tags.insert(t.asString());
        } else {
            for (const auto& t : str::splitTrimmed(uj["tags"].asString(), ',')) u.tags.insert(t);
        }
        if (uj.contains("limits")) u.limits = UserLimits::fromJson(uj["limits"]);
        if (!u.name.empty()) users_[u.name] = u;
    }
    for (const auto& pj : j["permissions"].asArray()) {
        Permission p;
        p.user = pj["user"].asString();
        p.vhost = pj["vhost"].asString("/");
        p.configure = pj["configure"].asString();
        p.write = pj["write"].asString();
        p.read = pj["read"].asString();
        permissions_[{p.user, p.vhost}] = p;
    }
    for (const auto& tj : j["topic_permissions"].asArray()) {
        TopicPermission p;
        p.user = tj["user"].asString();
        p.vhost = tj["vhost"].asString("/");
        p.exchange = tj["exchange"].asString();
        p.write = tj["write"].asString();
        p.read = tj["read"].asString();
        topicPermissions_[{p.user, p.vhost, p.exchange}] = p;
    }
}

}  // namespace tq::security
