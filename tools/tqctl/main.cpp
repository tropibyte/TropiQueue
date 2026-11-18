// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// tqctl - command-line administration through the management HTTP API.
#include <cstdlib>
#include <functional>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "common/crypto.h"
#include "common/fileio.h"
#include "common/json.h"
#include "common/platform.h"
#include "common/strings.h"
#include "net/http.h"

using tq::Json;

namespace {

struct Options {
    std::string host = "localhost";
    uint16_t port = 15672;
    std::string user = "guest";
    std::string password = "guest";
    std::string vhost = "/";
    bool json = false;
};

Options g_opts;

std::string vh() { return tq::str::urlEncode(g_opts.vhost); }
std::string enc(const std::string& s) { return tq::str::urlEncode(s); }

struct Result {
    int status = 0;
    Json body;
    std::string raw;
};

Result call(const std::string& method, const std::string& path, const Json& body = Json()) {
    std::map<std::string, std::string> headers;
    headers["Authorization"] = "Basic " + tq::crypto::base64Encode(g_opts.user + ":" + g_opts.password);
    std::string payload;
    if (!body.isNull()) {
        headers["Content-Type"] = "application/json";
        payload = body.dump();
    }
    auto r = tq::net::httpRequest(g_opts.host, g_opts.port, method, path, headers, payload);
    Result out;
    if (!r.error.empty()) {
        std::cerr << "tqctl: cannot reach " << g_opts.host << ":" << g_opts.port << ": " << r.error << "\n";
        std::exit(3);
    }
    out.status = r.status;
    out.raw = r.body;
    if (!r.body.empty()) {
        try {
            out.body = Json::parse(r.body);
        } catch (const tq::JsonError&) {
        }
    }
    if (out.status >= 400) {
        std::cerr << "Error (" << out.status << "): " << out.body["reason"].asString(r.body) << "\n";
        std::exit(1);
    }
    return out;
}

void printTable(const Json& rows, const std::vector<std::string>& columns) {
    if (g_opts.json) {
        std::cout << rows.dump(2) << "\n";
        return;
    }
    std::cout << tq::str::join(columns, "\t") << "\n";
    for (const auto& row : rows.asArray()) {
        std::vector<std::string> cells;
        for (const auto& c : columns) {
            const Json& v = row[c];
            cells.push_back(v.isString() ? v.asString() : v.isNull() ? "" : v.dump());
        }
        std::cout << tq::str::join(cells, "\t") << "\n";
    }
}

using Args = std::vector<std::string>;

void need(const Args& a, size_t n, const std::string& usage) {
    if (a.size() < n) {
        std::cerr << "Usage: tqctl " << usage << "\n";
        std::exit(2);
    }
}

Json tagsJson(const std::string& csv) {
    Json arr = Json::array();
    for (const auto& t : tq::str::splitTrimmed(csv, ',')) arr.push(t);
    return arr;
}

const std::map<std::string, std::pair<std::string, std::function<void(const Args&)>>>& commands() {
    static const std::map<std::string, std::pair<std::string, std::function<void(const Args&)>>> cmds = {
        {"status", {"status", [](const Args&) { std::cout << call("GET", "/api/overview").body.dump(2) << "\n"; }}},
        {"cluster_status",
         {"cluster_status", [](const Args&) { std::cout << call("GET", "/api/cluster").body.dump(2) << "\n"; }}},
        {"list_queues",
         {"list_queues", [](const Args&) {
              printTable(call("GET", "/api/queues/" + vh()).body,
                         {"name", "type", "durable", "messages_ready", "messages_unacknowledged", "consumers"});
          }}},
        {"list_exchanges",
         {"list_exchanges",
          [](const Args&) { printTable(call("GET", "/api/exchanges/" + vh()).body, {"name", "type", "durable"}); }}},
        {"list_bindings",
         {"list_bindings", [](const Args&) {
              printTable(call("GET", "/api/bindings/" + vh()).body,
                         {"source", "destination", "destination_type", "routing_key"});
          }}},
        {"list_connections",
         {"list_connections", [](const Args&) {
              printTable(call("GET", "/api/connections").body, {"name", "user", "vhost", "state", "channels"});
          }}},
        {"list_users", {"list_users", [](const Args&) { printTable(call("GET", "/api/users").body, {"name", "tags"}); }}},
        {"list_vhosts",
         {"list_vhosts", [](const Args&) { printTable(call("GET", "/api/vhosts").body, {"name", "messages", "queues"}); }}},
        {"list_policies",
         {"list_policies", [](const Args&) {
              printTable(call("GET", "/api/policies/" + vh()).body, {"name", "pattern", "apply-to", "priority"});
          }}},
        {"list_permissions",
         {"list_permissions", [](const Args&) {
              printTable(call("GET", "/api/permissions").body, {"user", "vhost", "configure", "write", "read"});
          }}},
        {"add_user",
         {"add_user <name> <password> [tags]",
          [](const Args& a) {
              need(a, 2, "add_user <name> <password> [tags]");
              Json b = Json::object();
              b["password"] = a[1];
              b["tags"] = tagsJson(a.size() > 2 ? a[2] : "");
              call("PUT", "/api/users/" + enc(a[0]), b);
              std::cout << "Adding user \"" << a[0] << "\" with password \"" << a[1] << "\" ...\n";
          }}},
        {"delete_user",
         {"delete_user <name>",
          [](const Args& a) {
              need(a, 1, "delete_user <name>");
              call("DELETE", "/api/users/" + enc(a[0]));
              std::cout << "Deleted user \"" << a[0] << "\"\n";
          }}},
        {"change_password",
         {"change_password <name> <password>",
          [](const Args& a) {
              need(a, 2, "change_password <name> <password>");
              Json existing = call("GET", "/api/users/" + enc(a[0])).body;
              Json b = Json::object();
              b["password"] = a[1];
              b["tags"] = existing["tags"];
              call("PUT", "/api/users/" + enc(a[0]), b);
              std::cout << "Changed password for user \"" << a[0] << "\"\n";
          }}},
        {"set_user_tags",
         {"set_user_tags <name> <tag,tag>",
          [](const Args& a) {
              need(a, 2, "set_user_tags <name> <tag,tag>");
              Json b = Json::object();
              b["tags"] = tagsJson(a[1]);
              call("PUT", "/api/users/" + enc(a[0]), b);
          }}},
        {"add_vhost",
         {"add_vhost <name>",
          [](const Args& a) {
              need(a, 1, "add_vhost <name>");
              call("PUT", "/api/vhosts/" + enc(a[0]));
              std::cout << "Added vhost \"" << a[0] << "\"\n";
          }}},
        {"delete_vhost",
         {"delete_vhost <name>",
          [](const Args& a) {
              need(a, 1, "delete_vhost <name>");
              call("DELETE", "/api/vhosts/" + enc(a[0]));
          }}},
        {"set_permissions",
         {"set_permissions <user> <configure> <write> <read>",
          [](const Args& a) {
              need(a, 4, "set_permissions <user> <configure> <write> <read>");
              Json b = Json::object();
              b["configure"] = a[1];
              b["write"] = a[2];
              b["read"] = a[3];
              call("PUT", "/api/permissions/" + vh() + "/" + enc(a[0]), b);
          }}},
        {"clear_permissions",
         {"clear_permissions <user>",
          [](const Args& a) {
              need(a, 1, "clear_permissions <user>");
              call("DELETE", "/api/permissions/" + vh() + "/" + enc(a[0]));
          }}},
        {"set_topic_permissions",
         {"set_topic_permissions <user> <exchange> <write> <read>",
          [](const Args& a) {
              need(a, 4, "set_topic_permissions <user> <exchange> <write> <read>");
              Json b = Json::object();
              b["exchange"] = a[1];
              b["write"] = a[2];
              b["read"] = a[3];
              call("PUT", "/api/topic-permissions/" + vh() + "/" + enc(a[0]), b);
          }}},
        {"set_policy",
         {"set_policy <name> <pattern> <definition-json> [priority] [apply-to]",
          [](const Args& a) {
              need(a, 3, "set_policy <name> <pattern> <definition-json> [priority] [apply-to]");
              Json b = Json::object();
              b["pattern"] = a[1];
              b["definition"] = Json::parse(a[2]);
              b["priority"] = a.size() > 3 ? tq::str::parseInt(a[3]).value_or(0) : 0;
              b["apply-to"] = a.size() > 4 ? a[4] : "all";
              call("PUT", "/api/policies/" + vh() + "/" + enc(a[0]), b);
          }}},
        {"clear_policy",
         {"clear_policy <name>",
          [](const Args& a) {
              need(a, 1, "clear_policy <name>");
              call("DELETE", "/api/policies/" + vh() + "/" + enc(a[0]));
          }}},
        {"declare_queue",
         {"declare_queue <name> [classic|quorum|stream] [durable=true]",
          [](const Args& a) {
              need(a, 1, "declare_queue <name> [classic|quorum|stream] [durable=true]");
              Json b = Json::object();
              b["durable"] = a.size() > 2 ? a[2] != "false" : true;
              Json args = Json::object();
              if (a.size() > 1) args["x-queue-type"] = a[1];
              b["arguments"] = args;
              call("PUT", "/api/queues/" + vh() + "/" + enc(a[0]), b);
          }}},
        {"delete_queue",
         {"delete_queue <name>",
          [](const Args& a) {
              need(a, 1, "delete_queue <name>");
              call("DELETE", "/api/queues/" + vh() + "/" + enc(a[0]));
          }}},
        {"purge_queue",
         {"purge_queue <name>",
          [](const Args& a) {
              need(a, 1, "purge_queue <name>");
              call("DELETE", "/api/queues/" + vh() + "/" + enc(a[0]) + "/contents");
          }}},
        {"declare_exchange",
         {"declare_exchange <name> <type>",
          [](const Args& a) {
              need(a, 2, "declare_exchange <name> <type>");
              Json b = Json::object();
              b["type"] = a[1];
              b["durable"] = true;
              call("PUT", "/api/exchanges/" + vh() + "/" + enc(a[0]), b);
          }}},
        {"bind_queue",
         {"bind_queue <exchange> <queue> [routing-key]",
          [](const Args& a) {
              need(a, 2, "bind_queue <exchange> <queue> [routing-key]");
              Json b = Json::object();
              b["routing_key"] = a.size() > 2 ? a[2] : "";
              call("POST", "/api/bindings/" + vh() + "/e/" + enc(a[0]) + "/q/" + enc(a[1]), b);
          }}},
        {"publish",
         {"publish <exchange> <routing-key> <payload>",
          [](const Args& a) {
              need(a, 3, "publish <exchange> <routing-key> <payload>");
              Json b = Json::object();
              b["routing_key"] = a[1];
              b["payload"] = a[2];
              b["payload_encoding"] = "string";
              b["properties"] = Json::object();
              std::string ex = a[0].empty() ? "amq.default" : a[0];
              Json r = call("POST", "/api/exchanges/" + vh() + "/" + enc(ex) + "/publish", b).body;
              std::cout << (r["routed"].asBool() ? "routed" : "NOT routed") << "\n";
          }}},
        {"get",
         {"get <queue> [count] [ackmode]",
          [](const Args& a) {
              need(a, 1, "get <queue> [count] [ackmode]");
              Json b = Json::object();
              b["count"] = a.size() > 1 ? tq::str::parseInt(a[1]).value_or(1) : 1;
              b["ackmode"] = a.size() > 2 ? a[2] : "ack_requeue_true";
              b["encoding"] = "auto";
              printTable(call("POST", "/api/queues/" + vh() + "/" + enc(a[0]) + "/get", b).body,
                         {"routing_key", "redelivered", "payload"});
          }}},
        {"close_connection",
         {"close_connection <name> [reason]",
          [](const Args& a) {
              need(a, 1, "close_connection <name> [reason]");
              call("DELETE", "/api/connections/" + enc(a[0]));
          }}},
        {"export_definitions",
         {"export_definitions <file>",
          [](const Args& a) {
              need(a, 1, "export_definitions <file>");
              Result r = call("GET", "/api/definitions");
              if (!tq::fs::writeFile(a[0], r.body.dump(2))) {
                  std::cerr << "cannot write " << a[0] << "\n";
                  std::exit(1);
              }
              std::cout << "Exported definitions to " << a[0] << "\n";
          }}},
        {"import_definitions",
         {"import_definitions <file>",
          [](const Args& a) {
              need(a, 1, "import_definitions <file>");
              auto text = tq::fs::readFile(a[0]);
              if (!text) {
                  std::cerr << "cannot read " << a[0] << "\n";
                  std::exit(1);
              }
              call("POST", "/api/definitions", Json::parse(*text));
              std::cout << "Imported definitions from " << a[0] << "\n";
          }}},
        {"add_super_stream",
         {"add_super_stream <name> [partitions]",
          [](const Args& a) {
              need(a, 1, "add_super_stream <name> [partitions]");
              Json b = Json::object();
              b["partitions"] = a.size() > 1 ? tq::str::parseInt(a[1]).value_or(3) : 3;
              call("PUT", "/api/super-streams/" + vh() + "/" + enc(a[0]), b);
          }}},
        {"list_parameters",
         {"list_parameters [component]",
          [](const Args& a) {
              std::string path = "/api/parameters";
              if (!a.empty()) path += "/" + enc(a[0]) + "/" + vh();
              printTable(call("GET", path).body, {"component", "vhost", "name", "value"});
          }}},
        {"set_parameter",
         {"set_parameter <component> <name> <value-json>",
          [](const Args& a) {
              need(a, 3, "set_parameter <component> <name> <value-json>");
              Json b = Json::object();
              b["value"] = Json::parse(a[2]);
              call("PUT", "/api/parameters/" + enc(a[0]) + "/" + vh() + "/" + enc(a[1]), b);
          }}},
        {"clear_parameter",
         {"clear_parameter <component> <name>",
          [](const Args& a) {
              need(a, 2, "clear_parameter <component> <name>");
              call("DELETE", "/api/parameters/" + enc(a[0]) + "/" + vh() + "/" + enc(a[1]));
          }}},
        {"set_global_parameter",
         {"set_global_parameter <name> <value-json>",
          [](const Args& a) {
              need(a, 2, "set_global_parameter <name> <value-json>");
              Json b = Json::object();
              b["value"] = Json::parse(a[1]);
              call("PUT", "/api/global-parameters/" + enc(a[0]), b);
          }}},
        {"set_cluster_name",
         {"set_cluster_name <name>",
          [](const Args& a) {
              need(a, 1, "set_cluster_name <name>");
              Json b = Json::object();
              b["value"] = a[0];
              call("PUT", "/api/global-parameters/cluster_name", b);
          }}},
        {"shovel_status",
         {"shovel_status",
          [](const Args&) {
              printTable(call("GET", "/api/shovels/" + vh()).body,
                         {"name", "state", "src_queue", "dest_queue", "dest_exchange", "messages_forwarded"});
          }}},
        {"restart_shovel",
         {"restart_shovel <name>",
          [](const Args& a) {
              need(a, 1, "restart_shovel <name>");
              call("DELETE", "/api/shovels/vhost/" + vh() + "/" + enc(a[0]) + "/restart");
          }}},
        {"federation_status",
         {"federation_status",
          [](const Args&) {
              printTable(call("GET", "/api/federation-links/" + vh()).body,
                         {"exchange", "upstream", "status", "messages_received", "error"});
          }}},
        {"set_user_limits",
         {"set_user_limits <user> <limits-json>",
          [](const Args& a) {
              need(a, 2, "set_user_limits <user> <limits-json>");
              Json limits = Json::parse(a[1]);
              for (const auto& [name, value] : limits.asObject()) {
                  Json b = Json::object();
                  b["value"] = value;
                  call("PUT", "/api/user-limits/" + enc(a[0]) + "/" + enc(name), b);
              }
          }}},
        {"clear_user_limits",
         {"clear_user_limits <user> <max-connections|max-channels|all>",
          [](const Args& a) {
              need(a, 2, "clear_user_limits <user> <max-connections|max-channels|all>");
              call("DELETE", "/api/user-limits/" + enc(a[0]) + "/" + enc(a[1]));
          }}},
        {"list_user_limits",
         {"list_user_limits [user]",
          [](const Args& a) {
              std::string path = "/api/user-limits";
              if (!a.empty()) path += "/" + enc(a[0]);
              printTable(call("GET", path).body, {"user", "value"});
          }}},
        {"list_feature_flags",
         {"list_feature_flags",
          [](const Args&) {
              printTable(call("GET", "/api/feature-flags").body, {"name", "state", "stability", "desc"});
          }}},
        {"enable_feature_flag",
         {"enable_feature_flag <name>",
          [](const Args& a) {
              need(a, 1, "enable_feature_flag <name>");
              call("PUT", "/api/feature-flags/" + enc(a[0]) + "/enable");
              std::cout << "Enabled feature flag " << a[0] << "\n";
          }}},
        {"trace_on",
         {"trace_on",
          [](const Args&) {
              Json b = Json::object();
              b["tracing"] = true;
              call("PUT", "/api/vhosts/" + vh() + "/tracing", b);
              std::cout << "Tracing enabled for vhost " << g_opts.vhost << "\n";
          }}},
        {"trace_off",
         {"trace_off",
          [](const Args&) {
              Json b = Json::object();
              b["tracing"] = false;
              call("PUT", "/api/vhosts/" + vh() + "/tracing", b);
              std::cout << "Tracing disabled for vhost " << g_opts.vhost << "\n";
          }}},
    };
    return cmds;
}

void usage() {
    std::cout << "Usage: tqctl [--host H] [--port P] [--user U] [--password P] [-p VHOST] [--json] <command> [args]\n\n"
                 "The password can also be supplied in the TQ_PASSWORD environment variable.\n\nCommands:\n";
    for (const auto& [name, cmd] : commands()) std::cout << "  " << cmd.first << "\n";
}

}  // namespace

int main(int argc, char** argv) {
    if (const char* pw = std::getenv("TQ_PASSWORD")) g_opts.password = pw;
    if (const char* user = std::getenv("TQ_USER")) g_opts.user = user;
    Args rest;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto value = [&]() -> std::string {
            if (i + 1 >= argc) {
                std::cerr << "missing value for " << a << "\n";
                std::exit(2);
            }
            return argv[++i];
        };
        if (a == "--host") g_opts.host = value();
        else if (a == "--port") g_opts.port = static_cast<uint16_t>(tq::str::parseInt(value()).value_or(15672));
        else if (a == "--user" || a == "-u") g_opts.user = value();
        else if (a == "--password") g_opts.password = value();
        else if (a == "-p" || a == "--vhost") g_opts.vhost = value();
        else if (a == "--json") g_opts.json = true;
        else if (a == "--help" || a == "-h") {
            usage();
            return 0;
        } else rest.push_back(a);
    }
    if (rest.empty()) {
        usage();
        return 2;
    }
    auto it = commands().find(rest[0]);
    if (it == commands().end()) {
        std::cerr << "tqctl: unknown command '" << rest[0] << "'\n";
        usage();
        return 2;
    }
    Args args(rest.begin() + 1, rest.end());
    try {
        it->second.second(args);
    } catch (const tq::JsonError& e) {
        std::cerr << "tqctl: invalid JSON: " << e.what() << "\n";
        return 2;
    }
    return 0;
}
