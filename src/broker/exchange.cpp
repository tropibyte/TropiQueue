// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/exchange.h"

#include <algorithm>
#include <string_view>

#include "common/crypto.h"
#include "common/random.h"
#include "common/strings.h"

namespace tq::broker {

Json Binding::toJson(const std::string& vhost) const {
    Json j = Json::object();
    j["source"] = source;
    j["vhost"] = vhost;
    j["destination"] = destination;
    j["destination_type"] = kind == DestinationKind::Queue ? "queue" : "exchange";
    j["routing_key"] = routingKey;
    j["arguments"] = arguments.toJson();
    return j;
}

std::optional<ExchangeType> parseExchangeType(const std::string& name) {
    if (name == "direct") return ExchangeType::Direct;
    if (name == "fanout") return ExchangeType::Fanout;
    if (name == "topic") return ExchangeType::Topic;
    if (name == "headers") return ExchangeType::Headers;
    if (name == "x-consistent-hash") return ExchangeType::ConsistentHash;
    if (name == "x-delayed-message") return ExchangeType::Delayed;
    if (name == "x-local-random") return ExchangeType::LocalRandom;
    return std::nullopt;
}

const char* exchangeTypeName(ExchangeType t) {
    switch (t) {
        case ExchangeType::Direct: return "direct";
        case ExchangeType::Fanout: return "fanout";
        case ExchangeType::Topic: return "topic";
        case ExchangeType::Headers: return "headers";
        case ExchangeType::ConsistentHash: return "x-consistent-hash";
        case ExchangeType::LocalRandom: return "x-local-random";
        case ExchangeType::Delayed: return "x-delayed-message";
    }
    return "direct";
}

Json ExchangeDef::toJson() const {
    Json j = Json::object();
    j["name"] = name;
    j["vhost"] = vhost;
    j["type"] = exchangeTypeName(type);
    j["durable"] = durable;
    j["auto_delete"] = autoDelete;
    j["internal"] = internal;
    j["arguments"] = arguments.toJson();
    return j;
}

std::optional<ExchangeDef> ExchangeDef::fromJson(const Json& j) {
    ExchangeDef d;
    d.name = j["name"].asString();
    d.vhost = j["vhost"].asString("/");
    auto type = parseExchangeType(j["type"].asString("direct"));
    if (!type) return std::nullopt;
    d.type = *type;
    d.durable = j["durable"].asBool(true);
    d.autoDelete = j["auto_delete"].asBool(false);
    d.internal = j["internal"].asBool(false);
    d.arguments = amqp::FieldTable::fromJson(j["arguments"]);
    return d;
}

// ------------------------------------------------------------------- Exchange

std::optional<std::string> Exchange::alternateExchange() const {
    return def_.arguments.getString("alternate-exchange");
}

bool Exchange::addBinding(const Binding& b) {
    for (const auto& existing : bindings_) {
        if (existing.sameAs(b)) return false;
    }
    bindings_.push_back(b);
    return true;
}

bool Exchange::removeBinding(const Binding& b) {
    for (auto it = bindings_.begin(); it != bindings_.end(); ++it) {
        if (it->sameAs(b)) {
            bindings_.erase(it);
            return true;
        }
    }
    return false;
}

size_t Exchange::removeBindingsTo(DestinationKind kind, const std::string& destination) {
    std::vector<Binding> victims;
    for (const auto& b : bindings_) {
        if (b.kind == kind && b.destination == destination) victims.push_back(b);
    }
    for (const auto& v : victims) removeBinding(v);  // virtual: subclasses update derived state
    return victims.size();
}

std::vector<std::string> Exchange::extraRoutingKeys(const Message& msg) {
    std::vector<std::string> keys;
    if (!msg.props.headers) return keys;
    for (const char* name : {"CC", "BCC"}) {
        const amqp::FieldValue* v = msg.props.headers->get(name);
        if (!v || !v->isArray() || !v->array) continue;
        for (const auto& e : *v->array) {
            if (auto s = e.asString()) keys.push_back(*s);
        }
    }
    return keys;
}

// ------------------------------------------------------------- topic matching

namespace {

std::vector<std::string_view> splitWords(std::string_view s) {
    std::vector<std::string_view> out;
    size_t start = 0;
    while (true) {
        size_t dot = s.find('.', start);
        if (dot == std::string_view::npos) {
            out.push_back(s.substr(start));
            break;
        }
        out.push_back(s.substr(start, dot - start));
        start = dot + 1;
    }
    return out;
}

bool matchWords(const std::vector<std::string_view>& p, size_t pi, const std::vector<std::string_view>& k,
                size_t ki) {
    if (pi == p.size()) return ki == k.size();
    if (p[pi] == "#") {
        if (pi + 1 == p.size()) return ki < k.size();
        for (size_t next = ki + 1; next <= k.size(); ++next) {
            if (matchWords(p, pi + 1, k, next)) return true;
        }
        return false;
    }
    if (ki == k.size()) return false;
    if (p[pi] == "*" || p[pi] == k[ki]) return matchWords(p, pi + 1, k, ki + 1);
    return false;
}

}  // namespace

bool topicMatches(const std::string& pattern, const std::string& routingKey) {
    if (pattern == "#") return true;
    auto p = splitWords(pattern);
    auto k = routingKey.empty() ? std::vector<std::string_view>{} : splitWords(routingKey);
    return matchWords(p, 0, k, 0);
}

// ------------------------------------------------------------ implementations

namespace {

class DirectExchange : public Exchange {
public:
    using Exchange::Exchange;
    void route(const Message& msg, const std::string& routingKey, std::vector<const Binding*>& out) const override {
        auto extra = extraRoutingKeys(msg);
        for (const auto& b : bindings_) {
            if (b.routingKey == routingKey ||
                std::find(extra.begin(), extra.end(), b.routingKey) != extra.end()) {
                out.push_back(&b);
            }
        }
    }
};

class FanoutExchange : public Exchange {
public:
    using Exchange::Exchange;
    void route(const Message&, const std::string&, std::vector<const Binding*>& out) const override {
        for (const auto& b : bindings_) out.push_back(&b);
    }
};

// Delivers each message to one bound queue chosen at random, ignoring the
// routing key. Used for request distribution where the requester does not
// care which instance answers.
class LocalRandomExchange : public Exchange {
public:
    using Exchange::Exchange;
    void route(const Message&, const std::string&, std::vector<const Binding*>& out) const override {
        std::vector<const Binding*> candidates;
        for (const auto& b : bindings_) {
            if (b.kind == DestinationKind::Queue) candidates.push_back(&b);
        }
        if (candidates.empty()) return;
        auto pick = rnd::uniform(0, static_cast<int64_t>(candidates.size()) - 1);
        out.push_back(candidates[static_cast<size_t>(pick)]);
    }
};

class TopicExchange : public Exchange {
public:
    using Exchange::Exchange;
    void route(const Message& msg, const std::string& routingKey, std::vector<const Binding*>& out) const override {
        auto extra = extraRoutingKeys(msg);
        for (const auto& b : bindings_) {
            bool hit = topicMatches(b.routingKey, routingKey);
            for (size_t i = 0; !hit && i < extra.size(); ++i) hit = topicMatches(b.routingKey, extra[i]);
            if (hit) out.push_back(&b);
        }
    }
};

class HeadersExchange : public Exchange {
public:
    using Exchange::Exchange;
    void route(const Message& msg, const std::string&, std::vector<const Binding*>& out) const override {
        static const amqp::FieldTable kEmpty;
        const amqp::FieldTable& headers = msg.props.headers ? *msg.props.headers : kEmpty;
        for (const auto& b : bindings_) {
            if (matches(b.arguments, headers)) out.push_back(&b);
        }
    }

private:
    static bool matches(const amqp::FieldTable& args, const amqp::FieldTable& headers) {
        std::string mode = args.getString("x-match").value_or("all");
        bool any = mode == "any" || mode == "any-with-x";
        bool includeX = mode == "all-with-x" || mode == "any-with-x";
        for (const auto& [name, expected] : args.entries()) {
            if (!includeX && str::startsWith(name, "x-")) continue;
            const amqp::FieldValue* actual = headers.get(name);
            if (any) {
                if (actual) return true;
            } else {
                if (!actual) return false;
                if (expected.type != 'V' && !(*actual == expected)) return false;
            }
        }
        return !any;
    }
};

class ConsistentHashExchange : public Exchange {
public:
    using Exchange::Exchange;

    bool addBinding(const Binding& b) override {
        if (!Exchange::addBinding(b)) return false;
        rebuildRing();
        return true;
    }
    bool removeBinding(const Binding& b) override {
        if (!Exchange::removeBinding(b)) return false;
        rebuildRing();
        return true;
    }

    void route(const Message& msg, const std::string& routingKey, std::vector<const Binding*>& out) const override {
        if (ring_.empty()) return;
        std::string key = routingKey;
        if (auto header = def_.arguments.getString("hash-header")) {
            const amqp::FieldValue* v = msg.props.headers ? msg.props.headers->get(*header) : nullptr;
            if (!v) return;
            key = v->asString().value_or(std::to_string(v->asInt().value_or(0)));
        } else if (auto prop = def_.arguments.getString("hash-property")) {
            if (*prop == "message_id") key = msg.props.messageId.value_or("");
            else if (*prop == "correlation_id") key = msg.props.correlationId.value_or("");
            else if (*prop == "timestamp") key = std::to_string(msg.props.timestamp.value_or(0));
        }
        uint32_t h = crypto::murmur3(key);
        auto it = std::lower_bound(ring_.begin(), ring_.end(), std::make_pair(h, size_t{0}));
        if (it == ring_.end()) it = ring_.begin();
        out.push_back(&bindings_[it->second]);
    }

private:
    void rebuildRing() {
        ring_.clear();
        for (size_t i = 0; i < bindings_.size(); ++i) {
            const Binding& b = bindings_[i];
            int weight = std::stoi(b.routingKey);
            for (int p = 0; p < weight; ++p) {
                ring_.emplace_back(crypto::murmur3(b.destination + "#" + std::to_string(p)), i);
            }
        }
        std::sort(ring_.begin(), ring_.end());
    }

    std::vector<std::pair<uint32_t, size_t>> ring_;
};

// x-delayed-message: routes like the type given in x-delayed-type. The delay
// itself is applied by the virtual host before routing.
class DelayedExchange : public Exchange {
public:
    explicit DelayedExchange(ExchangeDef def) : Exchange(def) {
        ExchangeDef inner = def;
        auto t = def.arguments.getString("x-delayed-type");
        auto parsed = t ? parseExchangeType(*t) : std::nullopt;
        inner.type = parsed && *parsed != ExchangeType::Delayed ? *parsed : ExchangeType::Direct;
        inner_ = makeExchange(inner);
    }
    bool addBinding(const Binding& b) override {
        if (!Exchange::addBinding(b)) return false;
        inner_->addBinding(b);
        return true;
    }
    bool removeBinding(const Binding& b) override {
        if (!Exchange::removeBinding(b)) return false;
        inner_->removeBinding(b);
        return true;
    }
    void route(const Message& msg, const std::string& routingKey, std::vector<const Binding*>& out) const override {
        std::vector<const Binding*> innerOut;
        inner_->route(msg, routingKey, innerOut);
        for (const Binding* ib : innerOut) {
            for (const auto& b : bindings_) {
                if (b.sameAs(*ib)) {
                    out.push_back(&b);
                    break;
                }
            }
        }
    }

private:
    std::unique_ptr<Exchange> inner_;
};

}  // namespace

std::unique_ptr<Exchange> makeExchange(ExchangeDef def) {
    switch (def.type) {
        case ExchangeType::Direct: return std::make_unique<DirectExchange>(std::move(def));
        case ExchangeType::Fanout: return std::make_unique<FanoutExchange>(std::move(def));
        case ExchangeType::Topic: return std::make_unique<TopicExchange>(std::move(def));
        case ExchangeType::Headers: return std::make_unique<HeadersExchange>(std::move(def));
        case ExchangeType::ConsistentHash: return std::make_unique<ConsistentHashExchange>(std::move(def));
        case ExchangeType::Delayed: return std::make_unique<DelayedExchange>(std::move(def));
        case ExchangeType::LocalRandom: return std::make_unique<LocalRandomExchange>(std::move(def));
    }
    return std::make_unique<DirectExchange>(std::move(def));
}

}  // namespace tq::broker
