// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "amqp/field_table.h"
#include "broker/message.h"
#include "common/json.h"

namespace tq::broker {

enum class DestinationKind { Queue, Exchange };

struct Binding {
    std::string source;
    DestinationKind kind = DestinationKind::Queue;
    std::string destination;
    std::string routingKey;
    amqp::FieldTable arguments;

    bool sameAs(const Binding& o) const {
        return source == o.source && kind == o.kind && destination == o.destination &&
               routingKey == o.routingKey && arguments == o.arguments;
    }
    Json toJson(const std::string& vhost) const;
};

enum class ExchangeType { Direct, Fanout, Topic, Headers, ConsistentHash, Delayed, LocalRandom };

std::optional<ExchangeType> parseExchangeType(const std::string& name);
const char* exchangeTypeName(ExchangeType t);

struct ExchangeDef {
    std::string name;
    std::string vhost;
    ExchangeType type = ExchangeType::Direct;
    bool durable = true;
    bool autoDelete = false;
    bool internal = false;
    amqp::FieldTable arguments;

    Json toJson() const;
    static std::optional<ExchangeDef> fromJson(const Json& j);
};

// Routing logic for one exchange. Exchanges are owned by a VirtualHost and are
// only mutated while the vhost topology lock is held exclusively.
class Exchange {
public:
    explicit Exchange(ExchangeDef def) : def_(std::move(def)) {}
    virtual ~Exchange() = default;

    const ExchangeDef& def() const { return def_; }
    const std::string& name() const { return def_.name; }
    ExchangeType type() const { return def_.type; }
    std::optional<std::string> alternateExchange() const;

    // Appends the bindings that match. The same destination may appear more
    // than once; callers de-duplicate.
    virtual void route(const Message& msg, const std::string& routingKey, std::vector<const Binding*>& out) const = 0;

    virtual bool addBinding(const Binding& b);
    virtual bool removeBinding(const Binding& b);
    // Removes all bindings that point at the given destination.
    size_t removeBindingsTo(DestinationKind kind, const std::string& destination);
    const std::vector<Binding>& bindings() const { return bindings_; }
    bool hasBindings() const { return !bindings_.empty(); }

    // Sender-selected distribution: extra routing keys from CC/BCC headers.
    static std::vector<std::string> extraRoutingKeys(const Message& msg);

protected:
    ExchangeDef def_;
    std::vector<Binding> bindings_;
};

std::unique_ptr<Exchange> makeExchange(ExchangeDef def);

// AMQP topic pattern matching: words are separated by '.', '*' matches one
// word and '#' matches zero or more words.
bool topicMatches(const std::string& pattern, const std::string& routingKey);

}  // namespace tq::broker
