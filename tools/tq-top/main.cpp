// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// tq-top - a live, top-style view of a broker's busiest queues and
// connections, built on the management API.
#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "common/crypto.h"
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
    std::string vhost;  // empty = all visible vhosts
    double intervalSec = 2.0;
    int iterations = 0;  // 0 = until interrupted
    size_t rows = 15;
    std::string sort = "rate";  // rate | messages | consumers | name
    bool batch = false;         // no screen clearing, for logs and pipes
};

volatile std::sig_atomic_t g_stop = 0;

void onSignal(int) { g_stop = 1; }

void usage() {
    std::cout << "Usage: tq-top [options]\n"
                 "\n"
                 "Shows the busiest queues and connections of a TropiQueue broker.\n"
                 "\n"
                 "Options:\n"
                 "  -H, --host HOST        management API host (default localhost)\n"
                 "  -P, --port PORT        management API port (default 15672)\n"
                 "  -u, --user USER        user name (default guest)\n"
                 "  -p, --password PASS    password (default guest, or $TQ_PASSWORD)\n"
                 "  -V, --vhost VHOST      only this virtual host\n"
                 "  -d, --delay SECONDS    refresh interval (default 2)\n"
                 "  -n, --iterations N     exit after N refreshes\n"
                 "  -r, --rows N           rows per table (default 15)\n"
                 "  -s, --sort KEY         rate, messages, consumers or name (default rate)\n"
                 "  -b, --batch            plain output without clearing the screen\n";
}

struct Sample {
    int64_t published = 0;
    int64_t delivered = 0;
    int64_t acked = 0;
};

struct QueueRow {
    std::string vhost;
    std::string name;
    std::string type;
    int64_t ready = 0;
    int64_t unacked = 0;
    int64_t consumers = 0;
    double publishRate = 0;
    double deliverRate = 0;
    double ackRate = 0;
};

class Client {
public:
    explicit Client(const Options& o) : o_(o) {
        auth_ = "Basic " + tq::crypto::base64Encode(o.user + ":" + o.password);
    }

    std::optional<Json> get(const std::string& path, std::string* error) {
        std::map<std::string, std::string> headers{{"Authorization", auth_}};
        auto r = tq::net::httpRequest(o_.host, o_.port, "GET", path, headers);
        if (!r.error.empty()) {
            *error = r.error;
            return std::nullopt;
        }
        if (r.status != 200) {
            *error = "HTTP " + std::to_string(r.status) + " for " + path;
            return std::nullopt;
        }
        try {
            return Json::parse(r.body);
        } catch (const tq::JsonError& e) {
            *error = e.what();
            return std::nullopt;
        }
    }

private:
    const Options& o_;
    std::string auth_;
};

std::string human(double v) {
    std::ostringstream s;
    if (v >= 1e9) s << std::fixed << std::setprecision(1) << v / 1e9 << "G";
    else if (v >= 1e6) s << std::fixed << std::setprecision(1) << v / 1e6 << "M";
    else if (v >= 1e4) s << std::fixed << std::setprecision(1) << v / 1e3 << "k";
    else if (v == static_cast<double>(static_cast<int64_t>(v))) s << static_cast<int64_t>(v);
    else s << std::fixed << std::setprecision(1) << v;
    return s.str();
}

std::string fit(const std::string& s, size_t width) {
    if (s.size() <= width) return s + std::string(width - s.size(), ' ');
    return s.substr(0, width - 1) + "~";
}

std::string rfit(const std::string& s, size_t width) {
    if (s.size() >= width) return s.substr(0, width);
    return std::string(width - s.size(), ' ') + s;
}

void sortRows(std::vector<QueueRow>& rows, const std::string& key) {
    std::stable_sort(rows.begin(), rows.end(), [&](const QueueRow& a, const QueueRow& b) {
        if (key == "messages") return a.ready + a.unacked > b.ready + b.unacked;
        if (key == "consumers") return a.consumers > b.consumers;
        if (key == "name") return a.name < b.name;
        return a.publishRate + a.deliverRate > b.publishRate + b.deliverRate;
    });
}

void render(const Options& o, const Json& overview, std::vector<QueueRow>& queues, const Json& connections,
            const std::string& warning) {
    std::ostringstream out;
    if (!o.batch) out << "\x1b[H\x1b[2J";
    const Json& totals = overview["queue_totals"];
    const Json& objects = overview["object_totals"];
    out << overview["product_name"].asString("TropiQueue") << " " << overview["product_version"].asString() << " on "
        << overview["node"].asString() << "  cluster " << overview["cluster_name"].asString() << "\n";
    out << "messages " << human(static_cast<double>(totals["messages"].asInt()))
        << "  ready " << human(static_cast<double>(totals["messages_ready"].asInt()))
        << "  unacked " << human(static_cast<double>(totals["messages_unacknowledged"].asInt()))
        << "  queues " << objects["queues"].asInt() << "  connections " << objects["connections"].asInt()
        << "  consumers " << objects["consumers"].asInt() << "\n";
    if (!warning.empty()) out << "! " << warning << "\n";
    out << "\n";

    sortRows(queues, o.sort);
    out << fit("VHOST", 12) << fit("QUEUE", 34) << fit("TYPE", 8) << rfit("READY", 9) << rfit("UNACK", 9)
        << rfit("CONS", 6) << rfit("PUB/s", 9) << rfit("DLV/s", 9) << rfit("ACK/s", 9) << "\n";
    size_t shown = 0;
    for (const auto& q : queues) {
        if (shown++ >= o.rows) break;
        out << fit(q.vhost, 12) << fit(q.name, 34) << fit(q.type, 8) << rfit(human(static_cast<double>(q.ready)), 9)
            << rfit(human(static_cast<double>(q.unacked)), 9) << rfit(std::to_string(q.consumers), 6)
            << rfit(human(q.publishRate), 9) << rfit(human(q.deliverRate), 9) << rfit(human(q.ackRate), 9) << "\n";
    }
    if (queues.size() > o.rows) out << "  ... " << queues.size() - o.rows << " more\n";

    out << "\n" << fit("CONNECTION", 44) << fit("USER", 14) << fit("VHOST", 12) << rfit("CHANNELS", 9)
        << rfit("RECV", 10) << rfit("SENT", 10) << "\n";
    std::vector<Json> conns(connections.asArray().begin(), connections.asArray().end());
    std::sort(conns.begin(), conns.end(), [](const Json& a, const Json& b) {
        return a["recv_oct"].asInt() + a["send_oct"].asInt() > b["recv_oct"].asInt() + b["send_oct"].asInt();
    });
    shown = 0;
    for (const auto& c : conns) {
        if (shown++ >= o.rows) break;
        out << fit(c["name"].asString(), 44) << fit(c["user"].asString(), 14) << fit(c["vhost"].asString(), 12)
            << rfit(std::to_string(c["channels"].asInt()), 9)
            << rfit(human(static_cast<double>(c["recv_oct"].asInt())) + "B", 10)
            << rfit(human(static_cast<double>(c["send_oct"].asInt())) + "B", 10) << "\n";
    }
    std::cout << out.str() << std::flush;
}

}  // namespace

int main(int argc, char** argv) {
    Options o;
    if (const char* pw = std::getenv("TQ_PASSWORD")) o.password = pw;
    std::vector<std::string> args(argv + 1, argv + argc);
    for (size_t i = 0; i < args.size(); ++i) {
        const std::string& a = args[i];
        auto value = [&]() -> std::string {
            if (i + 1 >= args.size()) {
                std::cerr << "tq-top: " << a << " needs a value\n";
                std::exit(2);
            }
            return args[++i];
        };
        if (a == "-h" || a == "--help") {
            usage();
            return 0;
        } else if (a == "-H" || a == "--host") {
            o.host = value();
        } else if (a == "-P" || a == "--port") {
            o.port = static_cast<uint16_t>(tq::str::parseInt(value()).value_or(15672));
        } else if (a == "-u" || a == "--user") {
            o.user = value();
        } else if (a == "-p" || a == "--password") {
            o.password = value();
        } else if (a == "-V" || a == "--vhost") {
            o.vhost = value();
        } else if (a == "-d" || a == "--delay") {
            try {
                o.intervalSec = std::max(0.2, std::stod(value()));
            } catch (const std::exception&) {
                std::cerr << "tq-top: invalid delay\n";
                return 2;
            }
        } else if (a == "-n" || a == "--iterations") {
            o.iterations = static_cast<int>(tq::str::parseInt(value()).value_or(0));
        } else if (a == "-r" || a == "--rows") {
            o.rows = static_cast<size_t>(std::max<int64_t>(1, tq::str::parseInt(value()).value_or(15)));
        } else if (a == "-s" || a == "--sort") {
            o.sort = value();
        } else if (a == "-b" || a == "--batch") {
            o.batch = true;
        } else {
            std::cerr << "tq-top: unknown option " << a << "\n";
            usage();
            return 2;
        }
    }

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    Client client(o);
    std::map<std::string, Sample> previous;
    auto lastSample = std::chrono::steady_clock::now();
    std::string queuesPath = o.vhost.empty() ? "/api/queues" : "/api/queues/" + tq::str::urlEncode(o.vhost);

    for (int iteration = 0; !g_stop && (o.iterations == 0 || iteration < o.iterations); ++iteration) {
        std::string error;
        auto overview = client.get("/api/overview", &error);
        auto queues = overview ? client.get(queuesPath, &error) : std::nullopt;
        auto connections = queues ? client.get("/api/connections", &error) : std::nullopt;
        if (!overview || !queues || !connections) {
            std::cerr << "tq-top: " << error << "\n";
            if (iteration == 0) return 1;
            std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(o.intervalSec * 1000)));
            continue;
        }
        auto now = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(now - lastSample).count();
        lastSample = now;

        std::vector<QueueRow> rows;
        std::map<std::string, Sample> current;
        for (const auto& q : queues->asArray()) {
            QueueRow row;
            row.vhost = q["vhost"].asString();
            row.name = q["name"].asString();
            row.type = q["type"].asString("classic");
            row.ready = q["messages_ready"].asInt();
            row.unacked = q["messages_unacknowledged"].asInt();
            row.consumers = q["consumers"].asInt();
            const Json& stats = q["message_stats"];
            Sample s{stats["publish"].asInt(), stats["deliver"].asInt(), stats["ack"].asInt()};
            std::string key = row.vhost + "/" + row.name;
            auto prev = previous.find(key);
            if (prev != previous.end() && elapsed > 0 && iteration > 0) {
                row.publishRate = std::max(0.0, static_cast<double>(s.published - prev->second.published) / elapsed);
                row.deliverRate = std::max(0.0, static_cast<double>(s.delivered - prev->second.delivered) / elapsed);
                row.ackRate = std::max(0.0, static_cast<double>(s.acked - prev->second.acked) / elapsed);
            }
            current[key] = s;
            rows.push_back(std::move(row));
        }
        previous.swap(current);
        std::string warning;
        auto nodes = client.get("/api/nodes", &error);
        if (nodes) {
            for (const auto& n : nodes->asArray()) {
                if (n["mem_alarm"].asBool()) warning = "memory alarm on " + n["name"].asString() + ": publishers are blocked";
            }
        }
        render(o, *overview, rows, *connections, warning);
        for (int slept = 0; !g_stop && slept < static_cast<int>(o.intervalSec * 10); ++slept) {
            if (o.iterations != 0 && iteration + 1 >= o.iterations) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    return 0;
}
