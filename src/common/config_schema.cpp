// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "common/config_schema.h"

#include <algorithm>
#include <sstream>

#include "common/strings.h"

namespace tq {

const char* configTypeName(ConfigType t) {
    switch (t) {
        case ConfigType::String: return "string";
        case ConfigType::Bool: return "boolean";
        case ConfigType::Int: return "integer";
        case ConfigType::Port: return "port";
        case ConfigType::Double: return "number";
        case ConfigType::Fraction: return "fraction";
        case ConfigType::Bytes: return "size";
        case ConfigType::Duration: return "duration";
        case ConfigType::Endpoint: return "endpoint";
        case ConfigType::List: return "list";
        case ConfigType::Enum: return "enum";
        case ConfigType::LogLevel: return "log level";
    }
    return "string";
}

size_t editDistance(const std::string& a, const std::string& b) {
    std::vector<size_t> prev(b.size() + 1), cur(b.size() + 1);
    for (size_t j = 0; j <= b.size(); ++j) prev[j] = j;
    for (size_t i = 1; i <= a.size(); ++i) {
        cur[0] = i;
        for (size_t j = 1; j <= b.size(); ++j) {
            size_t cost = a[i - 1] == b[j - 1] ? 0 : 1;
            cur[j] = std::min({prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost});
        }
        std::swap(prev, cur);
    }
    return prev[b.size()];
}

namespace {

using T = ConfigType;

ConfigKey key(std::string name, T type, std::string def, std::string desc) {
    ConfigKey k;
    k.name = std::move(name);
    k.type = type;
    k.defaultValue = std::move(def);
    k.description = std::move(desc);
    return k;
}

ConfigKey secretKey(std::string name, std::string desc) {
    ConfigKey k = key(std::move(name), T::String, "", std::move(desc));
    k.secret = true;
    return k;
}

std::vector<ConfigKey> brokerKeys() {
    return {
        // node
        key("node.name", T::String, "tropiqueue@localhost", "Name of this node, shown in the management API and logs."),
        key("node.id", T::Int, "1", "Numeric node id; must be unique within a cluster."),
        key("data_dir", T::String, "./data", "Directory for messages, queue indexes, streams and metadata."),
        key("pid_file", T::String, "", "Write the process id to this file while running."),
        // listeners
        key("listeners.tcp.default", T::Endpoint, "5672", "AMQP 0-9-1 listener."),
        key("listeners.max_connections_per_ip", T::Int, "0", "Maximum connections per client address (0 = unlimited)."),
        key("num_io_threads", T::Int, "2", "Number of network I/O threads."),
        key("connection_max", T::Int, "65536", "Maximum number of client connections."),
        // protocol
        key("channel_max", T::Int, "2047", "Maximum channels per connection offered in connection.tune."),
        key("frame_max", T::Bytes, "131072", "Maximum frame size offered in connection.tune."),
        key("heartbeat", T::Int, "60", "Heartbeat interval in seconds offered to clients (0 = disabled)."),
        key("max_message_size", T::Bytes, "128MB", "Largest message body accepted."),
        key("handshake_timeout", T::Duration, "10s", "Time a client has to complete the AMQP handshake."),
        key("consumer_timeout", T::Duration, "30m", "Close channels whose consumers hold a delivery longer than this."),
        // authentication
        key("default_user", T::String, "guest", "User created on first start."),
        secretKey("default_pass", "Password of the default user."),
        key("default_vhost", T::String, "/", "Virtual host created on first start."),
        key("loopback_users", T::List, "guest", "Users that may only connect from localhost (\"none\" to clear)."),
        key("auth_mechanisms", T::List, "PLAIN, AMQPLAIN", "SASL mechanisms offered to AMQP clients."),
        key("auth_oauth2.enabled", T::Bool, "false", "Accept OAuth 2.0 access tokens (JWT) in place of passwords."),
        key("auth_oauth2.resource_server_id", T::String, "tropiqueue", "Prefix of the scopes granted to this broker."),
        key("auth_oauth2.audience", T::String, "", "Required token audience (defaults to resource_server_id)."),
        key("auth_oauth2.issuer", T::String, "", "Required token issuer (optional)."),
        secretKey("auth_oauth2.default_key", "HS256 key used for tokens without a key id."),
        secretKey("auth_oauth2.signing_keys.*", "HS256 key for tokens with the given key id (kid)."),
        key("auth_oauth2.preferred_username_claim", T::String, "sub", "Claim that holds the user name."),
        key("auth_oauth2.leeway", T::Int, "60", "Clock skew tolerated when checking exp and nbf, in seconds."),
        // resources
        key("vm_memory_high_watermark.relative", T::Fraction, "0.4",
            "Block publishers when message memory exceeds this fraction of RAM."),
        key("vm_memory_high_watermark.absolute", T::Bytes, "0", "Absolute memory watermark; overrides relative."),
        key("tick_interval", T::Duration, "1s", "Interval of housekeeping (expiry, timeouts, alarms)."),
        key("store.sync_interval", T::Duration, "200ms", "How often the message store is flushed to disk."),
        key("store.segment_size", T::Bytes, "16MB", "Size of message store segment files."),
        // management
        key("management.enabled", T::Bool, "true", "Start the management HTTP API and web UI."),
        key("management.tcp.ip", T::String, "0.0.0.0", "Address the management API listens on."),
        key("management.tcp.port", T::Port, "15672", "Port of the management API."),
        key("management.cors.allow_origins", T::List, "", "Origins allowed to call the API from a browser."),
        key("management.www_dir", T::String, "www", "Directory containing the web UI files."),
        // clustering
        key("cluster.enabled", T::Bool, "false", "Run as a member of a Raft cluster."),
        key("cluster.nodes", T::List, "", "Cluster members as id@host:port, comma separated."),
        key("cluster.port", T::Port, "25672", "Port used for inter-node traffic."),
        secretKey("cluster.cookie", "Shared secret that nodes use to authenticate each other."),
        // protocols
        key("mqtt.enabled", T::Bool, "false", "Start the MQTT 3.1.1 listener."),
        key("mqtt.listeners.tcp.default", T::Endpoint, "1883", "MQTT listener."),
        key("mqtt.vhost", T::String, "/", "Virtual host used by MQTT clients."),
        key("mqtt.allow_anonymous", T::Bool, "true", "Let MQTT clients connect without credentials."),
        key("stomp.enabled", T::Bool, "false", "Start the STOMP 1.2 listener."),
        key("stomp.listeners.tcp.default", T::Endpoint, "61613", "STOMP listener."),
        key("stream.enabled", T::Bool, "false", "Start the binary stream protocol listener."),
        key("stream.listeners.tcp.default", T::Endpoint, "5552", "Stream protocol listener."),
        key("stream.advertised_host", T::String, "", "Host name returned to stream clients in metadata."),
        key("stream.advertised_port", T::Port, "0", "Port returned to stream clients (0 = listener port)."),
        key("stream.frame_max", T::Bytes, "1MB", "Largest stream protocol frame."),
        // features
        key("event_exchange.enabled", T::Bool, "true", "Publish internal events to amq.rabbitmq.event."),
        key("shovel.enabled", T::Bool, "true", "Run dynamic shovels defined as runtime parameters."),
        key("message_interceptors.incoming.set_header_timestamp.overwrite", T::Bool, "",
            "Add timestamp_in_ms (and the timestamp property) to published messages; true overwrites existing values."),
        key("message_interceptors.incoming.set_header_routing_node.overwrite", T::Bool, "",
            "Add x-routed-by with the node name to published messages; true overwrites an existing header."),
        key("load_definitions", T::String, "", "Definitions file (JSON) imported at every start."),
        key("federation.enabled", T::Bool, "true", "Run exchange federation links."),
        // logging
        key("log.file", T::String, "", "Log file (empty = standard error)."),
        key("log.level", T::LogLevel, "info", "debug, info, warning or error."),
    };
}

bool parseBool(const std::string& v) {
    std::string s = str::toLower(str::trim(v));
    return s == "true" || s == "false" || s == "yes" || s == "no" || s == "on" || s == "off" || s == "1" || s == "0";
}

bool parseDouble(const std::string& v, double* out) {
    try {
        size_t used = 0;
        double d = std::stod(v, &used);
        if (used != v.size()) return false;
        *out = d;
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

}  // namespace

const ConfigSchema& ConfigSchema::broker() {
    static const ConfigSchema schema(brokerKeys());
    return schema;
}

ConfigSchema::ConfigSchema(std::vector<ConfigKey> keys) : keys_(std::move(keys)) {}

const ConfigKey* ConfigSchema::find(const std::string& name) const {
    for (const auto& k : keys_) {
        if (k.name == name) return &k;
    }
    for (const auto& k : keys_) {
        if (str::endsWith(k.name, ".*")) {
            std::string prefix = k.name.substr(0, k.name.size() - 1);
            if (str::startsWith(name, prefix) && name.size() > prefix.size()) return &k;
        }
    }
    return nullptr;
}

std::string ConfigSchema::checkValue(const ConfigKey& k, const std::string& raw) {
    std::string v = str::trim(raw);
    switch (k.type) {
        case T::String:
        case T::List: return "";
        case T::Bool:
            return parseBool(v) ? "" : "expected true or false";
        case T::Int:
            return str::parseInt(v) ? "" : "expected an integer";
        case T::Port: {
            auto p = str::parseInt(v);
            if (!p || *p < 0 || *p > 65535) return "expected a port number (0-65535)";
            return "";
        }
        case T::Double: {
            double d = 0;
            return parseDouble(v, &d) ? "" : "expected a number";
        }
        case T::Fraction: {
            double d = 0;
            if (!parseDouble(v, &d) || d < 0.0 || d > 1.0) return "expected a number between 0 and 1";
            return "";
        }
        case T::Bytes:
            return str::parseByteSize(v) ? "" : "expected a size such as 4096, 512KB or 2GiB";
        case T::Duration:
            return str::parseDurationMs(v) ? "" : "expected a duration such as 250ms, 30s, 5m or 1h";
        case T::Endpoint: {
            std::string port = v;
            if (!v.empty() && v.front() == '[') {
                size_t close = v.find(']');
                if (close == std::string::npos) return "unterminated IPv6 address";
                port = close + 1 < v.size() && v[close + 1] == ':' ? v.substr(close + 2) : "";
            } else if (size_t colon = v.rfind(':'); colon != std::string::npos) {
                port = v.substr(colon + 1);
            }
            auto p = str::parseInt(port);
            if (!p || *p < 0 || *p > 65535) return "expected PORT, HOST:PORT or [IPV6]:PORT";
            return "";
        }
        case T::Enum:
            for (const auto& c : k.choices) {
                if (c == v) return "";
            }
            return "expected one of: " + str::join(k.choices, ", ");
        case T::LogLevel: {
            std::string l = str::toLower(v);
            if (l == "debug" || l == "info" || l == "warning" || l == "warn" || l == "error" || l == "none") return "";
            return "expected debug, info, warning, error or none";
        }
    }
    return "";
}

std::string ConfigSchema::suggest(const std::string& unknown) const {
    std::string best;
    size_t bestDist = std::string::npos;
    for (const auto& k : keys_) {
        std::string candidate = k.name;
        if (str::endsWith(candidate, ".*")) candidate = candidate.substr(0, candidate.size() - 2);
        size_t d = editDistance(unknown, candidate);
        if (d < bestDist) {
            bestDist = d;
            best = k.name;
        }
    }
    if (bestDist <= std::max<size_t>(2, unknown.size() / 5)) return best;
    return "";
}

std::vector<ConfigIssue> ConfigSchema::validate(const Config& cfg) const {
    std::vector<ConfigIssue> issues;
    for (const auto& [name, value] : cfg.all()) {
        const ConfigKey* k = find(name);
        if (!k) {
            ConfigIssue i;
            i.level = ConfigIssue::Level::Warning;
            i.key = name;
            i.message = "unknown configuration key";
            std::string hint = suggest(name);
            if (!hint.empty()) i.message += " (did you mean '" + hint + "'?)";
            issues.push_back(std::move(i));
            continue;
        }
        std::string problem = checkValue(*k, value);
        if (!problem.empty()) {
            ConfigIssue i;
            i.level = ConfigIssue::Level::Error;
            i.key = name;
            i.message = k->secret ? problem : "invalid value '" + value + "': " + problem;
            issues.push_back(std::move(i));
        }
    }
    return issues;
}

std::string ConfigSchema::reference() const {
    std::ostringstream out;
    out << "| Key | Type | Default | Description |\n";
    out << "|-----|------|---------|-------------|\n";
    for (const auto& k : keys_) {
        std::string type = configTypeName(k.type);
        if (k.type == T::Enum) type += " (" + str::join(k.choices, ", ") + ")";
        std::string def = k.secret ? "" : k.defaultValue;
        out << "| `" << k.name << "` | " << type << " | " << (def.empty() ? "" : "`" + def + "`") << " | "
            << k.description << " |\n";
    }
    return out.str();
}

std::string ConfigSchema::sampleConfig() const {
    std::ostringstream out;
    out << "# TropiQueue configuration. Every key is shown with its default value.\n";
    out << "# Values may reference environment variables as $(NAME).\n";
    std::string section;
    for (const auto& k : keys_) {
        std::string top = k.name.substr(0, k.name.find('.'));
        if (top != section) {
            out << "\n";
            section = top;
        }
        out << "# " << k.description << "\n";
        std::string name = k.name;
        if (str::endsWith(name, ".*")) name = name.substr(0, name.size() - 1) + "<id>";
        out << "# " << name << " = " << (k.secret ? "" : k.defaultValue) << "\n";
    }
    return out.str();
}

}  // namespace tq
