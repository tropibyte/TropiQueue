// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// tqd - the TropiQueue broker daemon.
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "broker/broker.h"
#include "common/config.h"
#include "common/config_schema.h"
#include "common/fileio.h"
#include "common/logging.h"
#include "common/platform.h"

#if defined(TQ_WINDOWS)
#include <process.h>
#define getpid _getpid
#else
#include <unistd.h>
#endif

namespace {

std::atomic<tq::broker::Broker*> g_broker{nullptr};
std::atomic<bool> g_stop{false};

void onSignal(int sig) {
    LOG_INFO("tqd") << "received signal " << sig << ", shutting down";
    if (auto* b = g_broker.load()) b->stop();
    g_stop.store(true);
}

void usage() {
    std::cout << "Usage: tqd [--config FILE] [--key=value ...]\n"
                 "\n"
                 "Starts a TropiQueue broker node.\n"
                 "\n"
                 "Options:\n"
                 "  --config FILE     configuration file (default: tropiqueue.conf if present,\n"
                 "                    or the TQ_CONFIG_FILE environment variable)\n"
                 "  --key=value       override any configuration key, e.g. --listeners.tcp.default=5673\n"
                 "  --check-config    validate the configuration and exit\n"
                 "  --list-config-keys\n"
                 "                    print every configuration key (Markdown) and exit\n"
                 "  --sample-config   print a commented sample tropiqueue.conf and exit\n"
                 "  --version         print the version and exit\n"
                 "  --help            show this help\n";
}

}  // namespace

int main(int argc, char** argv) {
    std::string configPath;
    std::vector<std::string> overrides;
    bool checkOnly = false;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--help" || a == "-h") {
            usage();
            return 0;
        }
        if (a == "--version") {
            std::cout << tq::kProductName << " " << tq::kProductVersion << "\n" << tq::kCopyright << "\n";
            return 0;
        }
        if (a == "--list-config-keys") {
            std::cout << tq::ConfigSchema::broker().reference();
            return 0;
        }
        if (a == "--sample-config") {
            std::cout << tq::ConfigSchema::broker().sampleConfig();
            return 0;
        }
        if (a == "--check-config") {
            checkOnly = true;
            continue;
        }
        if (a == "--config" && i + 1 < argc) {
            configPath = argv[++i];
            continue;
        }
        if (a.rfind("--config=", 0) == 0) {
            configPath = a.substr(9);
            continue;
        }
        overrides.push_back(a);
    }
    if (configPath.empty()) {
        if (const char* env = std::getenv("TQ_CONFIG_FILE")) configPath = env;
        else if (tq::fs::exists("tropiqueue.conf")) configPath = "tropiqueue.conf";
    }

    tq::Config cfg;
    if (!configPath.empty()) {
        std::vector<std::string> errors;
        cfg = tq::Config::fromFile(configPath, &errors);
        for (const auto& e : errors) std::cerr << configPath << ": " << e << "\n";
        if (!errors.empty() && !tq::fs::exists(configPath)) return 2;
    }
    cfg.applyOverrides(overrides);
    int configErrors = 0;
    for (const auto& issue : tq::ConfigSchema::broker().validate(cfg)) {
        bool error = issue.level == tq::ConfigIssue::Level::Error;
        configErrors += error ? 1 : 0;
        std::cerr << (configPath.empty() ? "configuration" : configPath) << ": " << (error ? "error" : "warning")
                  << ": " << issue.key << ": " << issue.message << "\n";
    }
    if (configErrors > 0) {
        std::cerr << "tqd: " << configErrors << " configuration error(s), not starting\n";
        return 2;
    }
    if (checkOnly) {
        std::cout << "configuration OK\n";
        return 0;
    }

    tq::broker::BrokerConfig bc = tq::broker::BrokerConfig::fromConfig(cfg);
    tq::broker::Broker broker(bc);
    std::string error;
    if (!broker.start(&error)) {
        std::cerr << "tqd: failed to start: " << error << "\n";
        return 1;
    }
    g_broker.store(&broker);

    std::string pidFile = cfg.getString("pid_file", "");
    if (!pidFile.empty()) tq::fs::writeFile(pidFile, std::to_string(getpid()) + "\n");

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    std::cout << tq::kProductName << " " << tq::kProductVersion << " running. AMQP port " << broker.amqpPort();
    if (broker.managementPort()) std::cout << ", management port " << broker.managementPort();
    if (broker.mqttPort()) std::cout << ", MQTT port " << broker.mqttPort();
    if (broker.stompPort()) std::cout << ", STOMP port " << broker.stompPort();
    if (broker.streamPort()) std::cout << ", stream port " << broker.streamPort();
    std::cout << std::endl;

    while (!g_stop.load() && broker.running()) std::this_thread::sleep_for(std::chrono::milliseconds(200));
    g_broker.store(nullptr);
    broker.stop();
    if (!pidFile.empty()) tq::fs::removeFile(pidFile);
    return 0;
}
