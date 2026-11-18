// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// tq-dump - inspect TropiQueue data files offline.
#include <cstdio>
#include <iostream>
#include <string>

#include "broker/message.h"
#include "cluster/raft.h"
#include "common/bytes.h"
#include "common/crypto.h"
#include "common/fileio.h"
#include "common/strings.h"
#include "storage/stream_log.h"

namespace {

void usage() {
    std::cout << "Usage: tq-dump <command> <path> [--bodies]\n\n"
                 "Commands:\n"
                 "  store <segment.tqs>     list records in a message store segment\n"
                 "  index <queue.idx>       list publish/deliver/ack records of a queue index\n"
                 "  stream <directory>      list chunks of every segment in a stream directory\n"
                 "  raft <directory>        show the hard state, snapshot and log of a Raft group\n";
}

std::string preview(const std::string& body, bool full) {
    if (full) return tq::str::escapeForLog(body, 1 << 20);
    return tq::str::escapeForLog(body, 60);
}

int dumpStore(const std::string& path, bool bodies) {
    auto data = tq::fs::readFile(path);
    if (!data) {
        std::cerr << "cannot read " << path << "\n";
        return 1;
    }
    size_t off = 0, n = 0;
    while (off + 12 <= data->size()) {
        uint32_t magic = tq::loadBe32(data->data() + off);
        uint32_t len = tq::loadBe32(data->data() + off + 4);
        uint32_t crc = tq::loadBe32(data->data() + off + 8);
        if (magic != 0x54514D53) {
            std::printf("%8zu: bad magic %08x, stopping\n", off, magic);
            break;
        }
        std::string payload = data->substr(off + 12, len);
        bool crcOk = tq::crypto::crc32c(payload) == crc;
        try {
            auto msg = tq::broker::Message::decode(payload);
            std::printf("%8zu: id=%llu exchange='%s' key='%s' size=%zu crc=%s %s\n", off,
                        static_cast<unsigned long long>(msg->id), msg->exchange.c_str(), msg->routingKey.c_str(),
                        msg->body.size(), crcOk ? "ok" : "BAD", preview(msg->body, bodies).c_str());
        } catch (const tq::DecodeError& e) {
            std::printf("%8zu: undecodable record (%s) crc=%s\n", off, e.what(), crcOk ? "ok" : "BAD");
        }
        off += 12 + len;
        ++n;
    }
    std::printf("%zu records, %zu bytes\n", n, data->size());
    return 0;
}

int dumpIndex(const std::string& path) {
    auto data = tq::fs::readFile(path);
    if (!data) {
        std::cerr << "cannot read " << path << "\n";
        return 1;
    }
    static const char* names[] = {"?", "publish", "deliver", "ack"};
    for (size_t off = 0; off + 17 <= data->size(); off += 17) {
        uint8_t type = static_cast<uint8_t>((*data)[off]);
        std::printf("%-8s tag=%llu msg=%llu\n", type <= 3 ? names[type] : "?",
                    static_cast<unsigned long long>(tq::loadBe64(data->data() + off + 1)),
                    static_cast<unsigned long long>(tq::loadBe64(data->data() + off + 9)));
    }
    if (data->size() % 17) std::printf("(%zu trailing bytes)\n", data->size() % 17);
    return 0;
}

int dumpStream(const std::string& dir, bool bodies) {
    tq::storage::StreamLog::Options opts;
    opts.directory = dir;
    tq::storage::StreamLog log(opts);
    std::string err;
    if (!log.open(&err)) {
        std::cerr << err << "\n";
        return 1;
    }
    auto st = log.stats();
    std::printf("segments=%llu bytes=%llu first=%llu next=%llu\n", static_cast<unsigned long long>(st.segments),
                static_cast<unsigned long long>(st.bytes), static_cast<unsigned long long>(st.firstOffset),
                static_cast<unsigned long long>(st.nextOffset));
    uint64_t offset = st.firstOffset;
    while (offset < st.nextOffset) {
        auto records = log.read(offset, 1000);
        if (records.empty()) break;
        for (const auto& r : records) {
            std::string body;
            try {
                body = tq::broker::Message::decode(r.data)->body;
            } catch (const tq::DecodeError&) {
                body = "<undecodable>";
            }
            std::printf("offset=%llu ts=%lld %s\n", static_cast<unsigned long long>(r.offset),
                        static_cast<long long>(r.timestamp), preview(body, bodies).c_str());
            offset = r.offset + 1;
        }
    }
    return 0;
}

int dumpRaft(const std::string& dir) {
    tq::cluster::RaftStorage storage(dir);
    std::string err;
    if (!storage.load(&err)) {
        std::cerr << err << "\n";
        return 1;
    }
    std::printf("term=%llu voted_for=%d snapshot_index=%llu snapshot_term=%llu last_index=%llu\n",
                static_cast<unsigned long long>(storage.currentTerm()), storage.votedFor(),
                static_cast<unsigned long long>(storage.snapshotIndex()),
                static_cast<unsigned long long>(storage.snapshotTerm()),
                static_cast<unsigned long long>(storage.lastIndex()));
    for (uint64_t i = storage.firstIndex(); i <= storage.lastIndex(); ++i) {
        const tq::cluster::LogEntry* e = storage.entry(i);
        if (!e) continue;
        std::printf("%8llu term=%llu bytes=%zu %s\n", static_cast<unsigned long long>(e->index),
                    static_cast<unsigned long long>(e->term), e->data.size(),
                    tq::str::escapeForLog(e->data, 60).c_str());
    }
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        usage();
        return 2;
    }
    std::string cmd = argv[1];
    std::string path = argv[2];
    bool bodies = argc > 3 && std::string(argv[3]) == "--bodies";
    if (cmd == "store") return dumpStore(path, bodies);
    if (cmd == "index") return dumpIndex(path);
    if (cmd == "stream") return dumpStream(path, bodies);
    if (cmd == "raft") return dumpRaft(path);
    usage();
    return 2;
}
