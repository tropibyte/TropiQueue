// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include "broker/broker.h"
#include "client/channel.h"
#include "client/connection.h"
#include "common/crypto.h"
#include "common/fileio.h"
#include "common/random.h"
#include "net/http.h"

namespace tq::test {

// A unique temporary directory that is removed when the object goes away.
class TempDir {
public:
    TempDir() {
        auto base = std::filesystem::temp_directory_path() / ("tq-test-" + rnd::uniqueName("", 12));
        path_ = base.string();
        fs::ensureDirectory(path_);
    }
    ~TempDir() { fs::removeTree(path_); }
    const std::string& path() const { return path_; }
    std::string file(const std::string& name) const { return fs::join(path_, name); }

private:
    std::string path_;
};

inline bool waitUntil(const std::function<bool()>& pred, int timeoutMs = 5000) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

inline broker::BrokerConfig testBrokerConfig(const std::string& dataDir) {
    broker::BrokerConfig cfg;
    cfg.dataDir = dataDir;
    cfg.amqpHost = "127.0.0.1";
    cfg.amqpPort = 0;
    cfg.managementHost = "127.0.0.1";
    cfg.managementPort = 0;
    cfg.heartbeat = 0;
    cfg.tickIntervalMs = 100;
    cfg.storeSyncIntervalMs = 100;
    cfg.memoryLimitBytes = 1LL << 40;
    cfg.ioThreads = 2;
    return cfg;
}

// Starts a broker on ephemeral ports in a temporary data directory.
class BrokerFixture {
public:
    explicit BrokerFixture(std::function<void(broker::BrokerConfig&)> tweak = nullptr) {
        cfg_ = testBrokerConfig(dir_.path());
        if (tweak) tweak(cfg_);
        broker_ = std::make_unique<broker::Broker>(cfg_);
        std::string err;
        if (!broker_->start(&err)) throw std::runtime_error("broker failed to start: " + err);
    }
    ~BrokerFixture() {
        if (broker_) broker_->stop();
    }

    broker::Broker& broker() { return *broker_; }
    const std::string& dataDir() const { return dir_.path(); }
    uint16_t port() const { return broker_->amqpPort(); }
    uint16_t httpPort() const { return broker_->managementPort(); }

    client::ConnectionParams params(const std::string& user = "guest", const std::string& pass = "guest",
                                    const std::string& vhost = "/") const {
        client::ConnectionParams p;
        p.host = "127.0.0.1";
        p.port = port();
        p.user = user;
        p.password = pass;
        p.vhost = vhost;
        p.heartbeat = 0;
        return p;
    }

    std::shared_ptr<client::Connection> connect(const std::string& user = "guest", const std::string& pass = "guest",
                                                const std::string& vhost = "/") {
        std::string err;
        auto c = client::Connection::open(params(user, pass, vhost), &err);
        if (!c) throw std::runtime_error("connect failed: " + err);
        return c;
    }

    net::HttpClientResponse http(const std::string& method, const std::string& path, const std::string& body = "",
                                 const std::string& user = "guest", const std::string& pass = "guest") {
        std::map<std::string, std::string> headers;
        if (!user.empty()) headers["Authorization"] = "Basic " + crypto::base64Encode(user + ":" + pass);
        if (!body.empty()) headers["Content-Type"] = "application/json";
        return net::httpRequest("127.0.0.1", httpPort(), method, path, headers, body);
    }

    void restart() {
        broker_->stop();
        broker_.reset();
        broker_ = std::make_unique<broker::Broker>(cfg_);
        std::string err;
        if (!broker_->start(&err)) throw std::runtime_error("broker failed to restart: " + err);
    }

private:
    TempDir dir_;
    broker::BrokerConfig cfg_;
    std::unique_ptr<broker::Broker> broker_;
};

}  // namespace tq::test
