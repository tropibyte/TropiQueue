// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// tq-perf - load generator in the spirit of RabbitMQ PerfTest.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "client/channel.h"
#include "client/connection.h"
#include "common/bytes.h"
#include "common/clock.h"
#include "common/logging.h"
#include "common/strings.h"

namespace {

struct Options {
    std::string uri = "amqp://guest:guest@localhost:5672";
    int producers = 1;
    int consumers = 1;
    int seconds = 10;
    size_t size = 1000;
    int rate = 0;  // messages per second per producer, 0 = unlimited
    std::string queue = "tq-perf";
    std::string queueType = "classic";
    bool persistent = false;
    bool confirm = false;
    int confirmBatch = 100;
    bool autoAck = false;
    uint16_t prefetch = 100;
    int multiAckEvery = 0;
    bool keepQueue = false;
};

std::atomic<uint64_t> g_published{0};
std::atomic<uint64_t> g_consumed{0};
std::atomic<uint64_t> g_confirmed{0};
std::atomic<bool> g_running{true};
std::mutex g_latMu;
std::vector<int64_t> g_latencies;

int64_t nowMicros() {
    using namespace std::chrono;
    return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
}

void producerLoop(const Options& o, int index) {
    std::string err;
    auto conn = tq::client::Connection::open(o.uri, &err);
    if (!conn) {
        std::cerr << "producer " << index << ": " << err << "\n";
        return;
    }
    auto ch = conn->openChannel();
    if (o.confirm) ch->confirmSelect();
    std::string body(o.size < 8 ? 8 : o.size, 'x');
    tq::amqp::BasicProperties props;
    if (o.persistent) props.deliveryMode = 2;
    int64_t interval = o.rate > 0 ? 1000000 / o.rate : 0;
    int64_t next = nowMicros();
    int sinceConfirm = 0;
    while (g_running.load()) {
        tq::storeBe64(body.data(), static_cast<uint64_t>(nowMicros()));
        try {
            ch->publish("", o.queue, body, props);
        } catch (const std::exception& e) {
            std::cerr << "producer " << index << ": " << e.what() << "\n";
            break;
        }
        g_published++;
        if (o.confirm && ++sinceConfirm >= o.confirmBatch) {
            if (ch->waitForConfirms(10000)) g_confirmed += static_cast<uint64_t>(sinceConfirm);
            sinceConfirm = 0;
        }
        if (interval > 0) {
            next += interval;
            int64_t sleep = next - nowMicros();
            if (sleep > 0) std::this_thread::sleep_for(std::chrono::microseconds(sleep));
        }
    }
    conn->close();
}

void consumerLoop(const Options& o, int index) {
    std::string err;
    auto conn = tq::client::Connection::open(o.uri, &err);
    if (!conn) {
        std::cerr << "consumer " << index << ": " << err << "\n";
        return;
    }
    auto ch = conn->openChannel();
    if (!o.autoAck) ch->qos(o.prefetch);
    std::atomic<int> unacked{0};
    std::weak_ptr<tq::client::Channel> weak = ch;
    ch->consume(
        o.queue,
        [&, weak](const tq::client::Delivery& d) {
            g_consumed++;
            if (d.body.size() >= 8) {
                int64_t sent = static_cast<int64_t>(tq::loadBe64(d.body.data()));
                std::lock_guard<std::mutex> lock(g_latMu);
                if (g_latencies.size() < 1000000) g_latencies.push_back(nowMicros() - sent);
            }
            if (o.autoAck) return;
            auto c = weak.lock();
            if (!c) return;
            if (o.multiAckEvery > 0) {
                if (++unacked >= o.multiAckEvery) {
                    c->ack(d.deliveryTag, true);
                    unacked = 0;
                }
            } else {
                c->ack(d.deliveryTag);
            }
        },
        o.autoAck);
    while (g_running.load()) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    conn->close();
}

void usage() {
    std::cout << "Usage: tq-perf [options]\n"
                 "  --uri URI               broker URI (default amqp://guest:guest@localhost:5672)\n"
                 "  -x, --producers N       producer connections (default 1)\n"
                 "  -y, --consumers N       consumer connections (default 1)\n"
                 "  -z, --time SECONDS      run time (default 10)\n"
                 "  -s, --size BYTES        message size (default 1000)\n"
                 "  -r, --rate N            per-producer rate limit (msg/s)\n"
                 "  -u, --queue NAME        queue name (default tq-perf)\n"
                 "  --queue-type TYPE       classic | quorum | stream\n"
                 "  -f persistent           publish persistent messages\n"
                 "  -c, --confirm N         publisher confirms every N messages\n"
                 "  -a, --autoack           consume with no-ack\n"
                 "  -q, --qos N             consumer prefetch (default 100)\n"
                 "  --multi-ack-every N     ack with multiple=true every N messages\n"
                 "  --keep-queue            do not delete the queue at the end\n";
}

}  // namespace

int main(int argc, char** argv) {
    Options o;
    tq::Logger::instance().setLevel(tq::LogLevel::Warning);
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
        auto num = [&]() { return tq::str::parseInt(next()).value_or(0); };
        if (a == "--help" || a == "-h") {
            usage();
            return 0;
        } else if (a == "--uri") o.uri = next();
        else if (a == "-x" || a == "--producers") o.producers = static_cast<int>(num());
        else if (a == "-y" || a == "--consumers") o.consumers = static_cast<int>(num());
        else if (a == "-z" || a == "--time") o.seconds = static_cast<int>(num());
        else if (a == "-s" || a == "--size") o.size = static_cast<size_t>(num());
        else if (a == "-r" || a == "--rate") o.rate = static_cast<int>(num());
        else if (a == "-u" || a == "--queue") o.queue = next();
        else if (a == "--queue-type") o.queueType = next();
        else if (a == "-f") o.persistent = next() == "persistent";
        else if (a == "-c" || a == "--confirm") {
            o.confirm = true;
            o.confirmBatch = std::max<int>(1, static_cast<int>(num()));
        } else if (a == "-a" || a == "--autoack") o.autoAck = true;
        else if (a == "-q" || a == "--qos") o.prefetch = static_cast<uint16_t>(num());
        else if (a == "--multi-ack-every") o.multiAckEvery = static_cast<int>(num());
        else if (a == "--keep-queue") o.keepQueue = true;
        else if (a == "--log-level") tq::Logger::instance().setLevel(tq::parseLogLevel(next()));
        else {
            std::cerr << "unknown option " << a << "\n";
            usage();
            return 2;
        }
    }

    std::string err;
    auto setup = tq::client::Connection::open(o.uri, &err);
    if (!setup) {
        std::cerr << "tq-perf: " << err << "\n";
        return 1;
    }
    {
        auto ch = setup->openChannel();
        tq::amqp::FieldTable args;
        if (o.queueType != "classic") args.set("x-queue-type", tq::amqp::FieldValue::longString(o.queueType));
        ch->declareQueue(o.queue, o.queueType != "classic" || o.persistent, false, false, args);
    }

    std::vector<std::thread> threads;
    for (int i = 0; i < o.consumers; ++i) threads.emplace_back(consumerLoop, std::cref(o), i);
    for (int i = 0; i < o.producers; ++i) threads.emplace_back(producerLoop, std::cref(o), i);

    uint64_t lastPub = 0, lastCons = 0;
    for (int s = 1; s <= o.seconds; ++s) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        uint64_t pub = g_published.load(), cons = g_consumed.load();
        std::printf("time %ds, sent %llu msg/s, received %llu msg/s\n", s,
                    static_cast<unsigned long long>(pub - lastPub), static_cast<unsigned long long>(cons - lastCons));
        lastPub = pub;
        lastCons = cons;
    }
    g_running.store(false);
    for (auto& t : threads) t.join();

    std::vector<int64_t> lat;
    {
        std::lock_guard<std::mutex> lock(g_latMu);
        lat = g_latencies;
    }
    std::sort(lat.begin(), lat.end());
    auto pct = [&](double p) -> long long {
        if (lat.empty()) return 0;
        return static_cast<long long>(lat[std::min(lat.size() - 1, static_cast<size_t>(p * static_cast<double>(lat.size())))]);
    };
    std::printf("summary: sent %llu, received %llu, confirmed %llu\n",
                static_cast<unsigned long long>(g_published.load()), static_cast<unsigned long long>(g_consumed.load()),
                static_cast<unsigned long long>(g_confirmed.load()));
    std::printf("latency (us): p50 %lld, p95 %lld, p99 %lld\n", pct(0.50), pct(0.95), pct(0.99));
    if (!o.keepQueue) {
        try {
            setup->openChannel()->deleteQueue(o.queue);
        } catch (const std::exception&) {
        }
    }
    setup->close();
    return 0;
}
