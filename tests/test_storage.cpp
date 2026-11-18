// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/message.h"
#include "common/clock.h"
#include "helpers.h"
#include "storage/message_store.h"
#include "storage/metadata_store.h"
#include "storage/queue_store.h"
#include "storage/stream_log.h"
#include "test.h"

using namespace tq;
using namespace tq::storage;

namespace {

broker::MessagePtr persistentMsg(const std::string& body) {
    amqp::BasicProperties p;
    p.deliveryMode = 2;
    return broker::makeMessage("", "q", p, body);
}

MessageStore::Options storeOptions(const std::string& dir, int64_t segmentBytes = 1 << 20) {
    MessageStore::Options o;
    o.directory = fs::join(dir, "msg_store");
    o.segmentBytes = segmentBytes;
    return o;
}

}  // namespace

// ------------------------------------------------------------ message store

TEST(message_store, write_read_remove) {
    test::TempDir dir;
    MessageStore store(storeOptions(dir.path()));
    std::string err;
    ASSERT_TRUE(store.open(&err));
    auto m = persistentMsg("hello");
    EXPECT_TRUE(store.addReference(m));
    EXPECT_TRUE(store.contains(m->id));
    auto back = store.read(m->id);
    ASSERT_TRUE(back != nullptr);
    EXPECT_EQ(back->body, "hello");
    EXPECT_TRUE(back->persistent());
    store.removeReference(m->id);
    EXPECT_FALSE(store.contains(m->id));
    EXPECT_TRUE(store.read(m->id) == nullptr);
}

TEST(message_store, recovery_and_unreferenced_cleanup) {
    test::TempDir dir;
    auto keep = persistentMsg("keep");
    auto drop = persistentMsg("drop");
    {
        MessageStore store(storeOptions(dir.path()));
        std::string err;
        ASSERT_TRUE(store.open(&err));
        store.addReference(keep);
        store.addReference(drop);
        store.sync();
    }
    MessageStore store(storeOptions(dir.path()));
    std::string err;
    ASSERT_TRUE(store.open(&err));
    EXPECT_EQ(store.stats().messages, 2u);
    EXPECT_TRUE(store.restoreReference(keep->id));
    EXPECT_EQ(store.dropUnreferenced(), 1u);
    EXPECT_TRUE(store.contains(keep->id));
    EXPECT_FALSE(store.contains(drop->id));
    EXPECT_GE(store.stats().maxMessageId, drop->id);
}

TEST(message_store, torn_tail_is_truncated) {
    test::TempDir dir;
    auto m = persistentMsg("complete");
    {
        MessageStore store(storeOptions(dir.path()));
        std::string err;
        ASSERT_TRUE(store.open(&err));
        store.addReference(m);
        store.sync();
    }
    std::string seg = fs::join(fs::join(dir.path(), "msg_store"), "00000000.tqs");
    auto data = fs::readFile(seg);
    ASSERT_TRUE(data.has_value());
    fs::writeFile(seg, *data + std::string("\x54\x51\x4D\x53\x00\x00\x00\x40junk", 12));
    MessageStore store(storeOptions(dir.path()));
    std::string err;
    ASSERT_TRUE(store.open(&err));
    EXPECT_TRUE(store.contains(m->id));
    EXPECT_EQ(fs::fileSize(seg), static_cast<int64_t>(data->size()));
}

TEST(message_store, compaction_reclaims_space) {
    test::TempDir dir;
    MessageStore store(storeOptions(dir.path(), 4096));
    std::string err;
    ASSERT_TRUE(store.open(&err));
    std::vector<broker::MessagePtr> msgs;
    for (int i = 0; i < 60; ++i) {
        msgs.push_back(persistentMsg(std::string(200, static_cast<char>('a' + i % 26))));
        store.addReference(msgs.back());
    }
    for (size_t i = 0; i < msgs.size(); ++i) {
        if (i % 4 != 0) store.removeReference(msgs[i]->id);
    }
    store.flush();
    EXPECT_GT(store.stats().garbageBytes, 0u);
    EXPECT_GT(store.compact(), 0u);
    for (size_t i = 0; i < msgs.size(); i += 4) {
        auto back = store.read(msgs[i]->id);
        ASSERT_TRUE(back != nullptr);
        EXPECT_EQ(back->body, msgs[i]->body);
    }
}

// -------------------------------------------------------------- queue store

TEST(queue_store, publish_ack_recover) {
    test::TempDir dir;
    auto m1 = persistentMsg("one");
    auto m2 = persistentMsg("two");
    auto m3 = persistentMsg("three");
    std::string path = QueueStore::pathFor(dir.path(), "/", "orders");
    {
        MessageStore store(storeOptions(dir.path()));
        std::string err;
        ASSERT_TRUE(store.open(&err));
        QueueStore qs(store, path);
        qs.recordPublish(1, m1);
        qs.recordPublish(2, m2);
        qs.recordPublish(3, m3);
        qs.recordAck(2);
        qs.flush();
        store.sync();
    }
    MessageStore store(storeOptions(dir.path()));
    std::string err;
    ASSERT_TRUE(store.open(&err));
    QueueStore qs(store, path);
    auto rec = qs.recover();
    ASSERT_EQ(rec.size(), 2u);
    EXPECT_EQ(rec[0].tag, 1u);
    EXPECT_EQ(rec[0].msg->body, "one");
    EXPECT_EQ(rec[1].msg->body, "three");
    EXPECT_EQ(store.dropUnreferenced(), 1u);  // the acknowledged message
    qs.destroy();
    EXPECT_FALSE(fs::exists(path));
    EXPECT_EQ(store.stats().messages, 0u);
}

TEST(queue_store, path_layout) {
    std::string p = QueueStore::pathFor("/data", "/", "orders");
    EXPECT_TRUE(p.find("default") != std::string::npos);
    EXPECT_TRUE(p.find("orders.idx") != std::string::npos);
}

// --------------------------------------------------------------- stream log

TEST(stream_log, append_and_read) {
    test::TempDir dir;
    StreamLog::Options o;
    o.directory = dir.file("s");
    StreamLog log(o);
    std::string err;
    ASSERT_TRUE(log.open(&err));
    EXPECT_EQ(log.append({"a", "b"}, 1000), 0u);
    EXPECT_EQ(log.append({"c"}, 2000), 2u);
    EXPECT_EQ(log.nextOffset(), 3u);
    auto recs = log.read(1, 10);
    ASSERT_EQ(recs.size(), 2u);
    EXPECT_EQ(recs[0].offset, 1u);
    EXPECT_EQ(recs[0].data, "b");
    EXPECT_EQ(recs[1].data, "c");
    EXPECT_TRUE(log.read(3, 10).empty());
    EXPECT_EQ(log.offsetForTimestamp(1500), 2u);
    EXPECT_EQ(log.offsetForTimestamp(5000), 3u);
}

TEST(stream_log, segments_roll_and_recover) {
    test::TempDir dir;
    StreamLog::Options o;
    o.directory = dir.file("s");
    o.segmentBytes = 300;
    {
        StreamLog log(o);
        std::string err;
        ASSERT_TRUE(log.open(&err));
        for (int i = 0; i < 20; ++i) log.append({std::string(50, 'x') + std::to_string(i)}, 1000 + i);
        EXPECT_GT(log.stats().segments, 2u);
        log.flush();
    }
    StreamLog log(o);
    std::string err;
    ASSERT_TRUE(log.open(&err));
    EXPECT_EQ(log.nextOffset(), 20u);
    auto recs = log.read(0, 100);
    ASSERT_EQ(recs.size(), 20u);
    for (size_t i = 0; i < recs.size(); ++i) {
        EXPECT_EQ(recs[i].offset, i);
        EXPECT_EQ(recs[i].data, std::string(50, 'x') + std::to_string(i));
    }
    auto mid = log.read(13, 3);
    ASSERT_EQ(mid.size(), 3u);
    EXPECT_EQ(mid[0].offset, 13u);
}

TEST(stream_log, size_retention_keeps_newest) {
    test::TempDir dir;
    StreamLog::Options o;
    o.directory = dir.file("s");
    o.segmentBytes = 200;
    o.maxBytes = 400;
    StreamLog log(o);
    std::string err;
    ASSERT_TRUE(log.open(&err));
    for (int i = 0; i < 30; ++i) log.append({std::string(40, 'y')}, wallClockMs());
    size_t removed = log.applyRetention(wallClockMs());
    EXPECT_GT(removed, 0u);
    EXPECT_GT(log.firstOffset(), 0u);
    EXPECT_EQ(log.nextOffset(), 30u);
    auto recs = log.read(0, 100);
    ASSERT_FALSE(recs.empty());
    EXPECT_EQ(recs.front().offset, log.firstOffset());
    EXPECT_EQ(recs.back().offset, 29u);
}

TEST(stream_log, manager_offsets) {
    test::TempDir dir;
    {
        StreamManager mgr(dir.file("streams"));
        std::string err;
        auto log = mgr.open("/", "events", StreamLog::Options{}, &err);
        ASSERT_TRUE(log != nullptr);
        EXPECT_TRUE(mgr.find("/", "events") == log);
        mgr.storeOffset("/", "events", "reader-1", 41);
        mgr.flushOffsets();
    }
    StreamManager mgr(dir.file("streams"));
    EXPECT_EQ(mgr.queryOffset("/", "events", "reader-1").value_or(0), 41u);
    EXPECT_FALSE(mgr.queryOffset("/", "events", "reader-2").has_value());
}

// ----------------------------------------------------------- metadata store

TEST(metadata_store, save_and_load) {
    test::TempDir dir;
    MetadataStore store(dir.file("metadata.json"));
    std::string err;
    EXPECT_FALSE(store.load(&err).has_value());
    EXPECT_TRUE(err.empty());
    Json snap = Json::object();
    snap["vhosts"].push(Json::object());
    EXPECT_TRUE(store.save(snap));
    auto back = store.load(&err);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ((*back)["vhosts"].size(), 1u);
    EXPECT_EQ((*back)["snapshot_version"].asInt(), 1);
    fs::writeFile(dir.file("metadata.json"), "{broken");
    EXPECT_FALSE(store.load(&err).has_value());
    EXPECT_FALSE(err.empty());
}
