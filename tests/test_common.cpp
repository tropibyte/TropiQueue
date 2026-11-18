// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include <atomic>
#include <chrono>
#include <thread>

#include "common/bytes.h"
#include "common/config.h"
#include "common/crypto.h"
#include "common/executor.h"
#include "common/fileio.h"
#include "common/json.h"
#include "common/metrics.h"
#include "common/strings.h"
#include "helpers.h"
#include "test.h"

using namespace tq;

// ------------------------------------------------------------------- bytes

TEST(bytes, round_trip_integers) {
    ByteWriter w;
    w.u8(0xAB);
    w.u16(0x1234);
    w.u32(0xDEADBEEF);
    w.u64(0x0102030405060708ULL);
    w.i32(-5);
    w.f64(3.5);
    ByteReader r(w.data());
    EXPECT_EQ(r.u8(), 0xAB);
    EXPECT_EQ(r.u16(), 0x1234);
    EXPECT_EQ(r.u32(), 0xDEADBEEFu);
    EXPECT_EQ(r.u64(), 0x0102030405060708ULL);
    EXPECT_EQ(r.i32(), -5);
    EXPECT_EQ(r.f64(), 3.5);
    EXPECT_TRUE(r.atEnd());
}

TEST(bytes, big_endian_layout) {
    ByteWriter w;
    w.u32(0x01020304);
    ASSERT_EQ(w.size(), 4u);
    EXPECT_EQ(static_cast<int>(w.data()[0]), 1);
    EXPECT_EQ(static_cast<int>(w.data()[3]), 4);
}

TEST(bytes, strings) {
    ByteWriter w;
    w.shortString("queue");
    w.longString(std::string(1000, 'x'));
    ByteReader r(w.data());
    EXPECT_EQ(r.shortString(), "queue");
    EXPECT_EQ(r.longString().size(), 1000u);
}

TEST(bytes, underrun_throws) {
    std::string data("\x00\x01", 2);
    ByteReader r(data);
    EXPECT_THROW(r.u32(), DecodeError);
    ByteReader r2(data);
    EXPECT_THROW(r2.bytes(3), DecodeError);
}

TEST(bytes, placeholder_patch) {
    ByteWriter w;
    size_t pos = w.placeholderU32();
    w.bytes("hello");
    w.patchU32(pos, 5);
    ByteReader r(w.data());
    EXPECT_EQ(r.longString(), "hello");
}

// ----------------------------------------------------------------- strings

TEST(strings, split_trim_join) {
    auto parts = str::split("a,b,,c", ',');
    ASSERT_EQ(parts.size(), 4u);
    EXPECT_EQ(parts[2], "");
    auto trimmed = str::splitTrimmed(" a , b ,, c ", ',');
    ASSERT_EQ(trimmed.size(), 3u);
    EXPECT_EQ(trimmed[1], "b");
    EXPECT_EQ(str::join(trimmed, "|"), "a|b|c");
    EXPECT_EQ(str::trim("  x \t"), "x");
}

TEST(strings, case_and_prefix) {
    EXPECT_EQ(str::toLower("AbC"), "abc");
    EXPECT_TRUE(str::startsWith("amq.topic", "amq."));
    EXPECT_FALSE(str::startsWith("am", "amq."));
    EXPECT_TRUE(str::endsWith("file.idx", ".idx"));
    EXPECT_TRUE(str::iequals("Hello", "hELLO"));
    EXPECT_EQ(str::replaceAll("a-b-c", "-", "+"), "a+b+c");
}

TEST(strings, parse_numbers) {
    EXPECT_EQ(str::parseInt("42").value_or(0), 42);
    EXPECT_EQ(str::parseInt("-7").value_or(0), -7);
    EXPECT_FALSE(str::parseInt("12x").has_value());
    EXPECT_FALSE(str::parseUint("-1").has_value());
    EXPECT_EQ(str::parseDouble("0.4").value_or(0), 0.4);
    EXPECT_EQ(str::parseBool("true").value_or(false), true);
    EXPECT_FALSE(str::parseBool("maybe").has_value());
}

TEST(strings, byte_sizes_and_durations) {
    EXPECT_EQ(str::parseByteSize("1024").value_or(0), 1024);
    EXPECT_EQ(str::parseByteSize("128MB").value_or(0), 128LL * 1000 * 1000);
    EXPECT_EQ(str::parseByteSize("512KiB").value_or(0), 512LL * 1024);
    EXPECT_FALSE(str::parseByteSize("lots").has_value());
    EXPECT_EQ(str::parseDurationMs("30s").value_or(0), 30000);
    EXPECT_EQ(str::parseDurationMs("5m").value_or(0), 300000);
    EXPECT_EQ(str::parseDurationMs("250").value_or(0), 250);
}

TEST(strings, url_and_hex) {
    EXPECT_EQ(str::urlEncode("/"), "%2F");
    EXPECT_EQ(str::urlDecode("%2Fvhost").value_or(""), "/vhost");
    EXPECT_FALSE(str::urlDecode("%zz").has_value());
    EXPECT_EQ(str::hexEncode("\x01\xff"), "01ff");
    EXPECT_EQ(str::hexDecode("01ff").value_or(""), std::string("\x01\xff"));
    EXPECT_EQ(str::escapeForLog("a\nb"), "a\\x0ab");
}

// ------------------------------------------------------------------ crypto

TEST(crypto, sha256_vectors) {
    EXPECT_EQ(str::hexEncode(crypto::sha256("abc")),
              "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    EXPECT_EQ(str::hexEncode(crypto::sha256("")),
              "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    std::string million(1000000, 'a');
    EXPECT_EQ(str::hexEncode(crypto::sha256(million)),
              "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
}

TEST(crypto, hmac_rfc4231_case2) {
    EXPECT_EQ(str::hexEncode(crypto::hmacSha256("Jefe", "what do ya want for nothing?")),
              "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
}

TEST(crypto, pbkdf2_vector) {
    EXPECT_EQ(str::hexEncode(crypto::pbkdf2Sha256("password", "salt", 1, 32)),
              "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b");
}

TEST(crypto, crc32c_check_value) { EXPECT_EQ(crypto::crc32c("123456789"), 0xE3069283u); }

TEST(crypto, base64_round_trip) {
    for (std::string s : {"", "f", "fo", "foo", "foob", "fooba", "foobar"}) {
        EXPECT_EQ(crypto::base64Decode(crypto::base64Encode(s)).value_or("!"), s);
    }
    EXPECT_EQ(crypto::base64Encode("foobar"), "Zm9vYmFy");
    EXPECT_FALSE(crypto::base64Decode("Zm9v!mFy").has_value());
}

TEST(crypto, murmur3_vectors) {
    EXPECT_EQ(crypto::murmur3("", 0), 0u);
    EXPECT_EQ(crypto::murmur3("hello", 0), 0x248bfa47u);
}

TEST(crypto, constant_time_equals) {
    EXPECT_TRUE(crypto::constantTimeEquals("secret", "secret"));
    EXPECT_FALSE(crypto::constantTimeEquals("secret", "secreT"));
    EXPECT_FALSE(crypto::constantTimeEquals("secret", "secrets"));
}

// -------------------------------------------------------------------- json

TEST(json, parse_and_dump) {
    Json j = Json::parse(R"({"a":1,"b":[true,false,null],"c":{"d":"x\"y"},"e":2.5})");
    EXPECT_EQ(j["a"].asInt(), 1);
    EXPECT_EQ(j["b"].size(), 3u);
    EXPECT_EQ(j["c"]["d"].asString(), "x\"y");
    EXPECT_EQ(j["e"].asDouble(), 2.5);
    Json again = Json::parse(j.dump());
    EXPECT_TRUE(again == j);
}

TEST(json, unicode_escapes) {
    Json j = Json::parse(R"("café 😀")");
    EXPECT_EQ(j.asString(), "caf\xc3\xa9 \xf0\x9f\x98\x80");
}

TEST(json, errors) {
    EXPECT_THROW(Json::parse("{"), JsonError);
    EXPECT_THROW(Json::parse("[1,]"), JsonError);
    EXPECT_THROW(Json::parse("tru"), JsonError);
    EXPECT_THROW(Json::parse("{} x"), JsonError);
}

TEST(json, building) {
    Json j = Json::object();
    j["name"] = "orders";
    j["count"] = 3;
    j["list"].push(1);
    j["list"].push("two");
    EXPECT_EQ(j.dump(), R"({"count":3,"list":[1,"two"],"name":"orders"})");
    EXPECT_TRUE(j.contains("name"));
    j.erase("name");
    EXPECT_FALSE(j.contains("name"));
}

// ------------------------------------------------------------------ config

TEST(config, parse_file_format) {
    std::vector<std::string> errors;
    Config c = Config::fromString(
        "# comment\nlisteners.tcp.default = 5673\nvm_memory_high_watermark.relative = 0.6\n"
        "default_user = \"admin\"\nloopback_users = guest, ops\nbroken line\n",
        &errors);
    EXPECT_EQ(errors.size(), 1u);
    EXPECT_EQ(c.getInt("listeners.tcp.default", 0), 5673);
    EXPECT_EQ(c.getDouble("vm_memory_high_watermark.relative", 0), 0.6);
    EXPECT_EQ(c.getString("default_user"), "admin");
    EXPECT_EQ(c.getList("loopback_users").size(), 2u);
    EXPECT_EQ(c.withPrefix("listeners.").size(), 1u);
}

TEST(config, typed_getters_and_overrides) {
    Config c = Config::fromString("max_message_size = 16MiB\nconsumer_timeout = 15m\nmanagement.enabled = false\n");
    EXPECT_EQ(c.getBytes("max_message_size", 0), 16LL * 1024 * 1024);
    EXPECT_EQ(c.getDurationMs("consumer_timeout", 0), 15LL * 60 * 1000);
    EXPECT_FALSE(c.getBool("management.enabled", true));
    EXPECT_EQ(c.getInt("missing", 7), 7);
    c.applyOverrides({"--management.enabled=true", "ignored"});
    EXPECT_TRUE(c.getBool("management.enabled", false));
}

// ---------------------------------------------------------------- executor

TEST(executor, thread_pool_runs_tasks) {
    ThreadPool pool(3, "test");
    std::atomic<int> n{0};
    for (int i = 0; i < 100; ++i) pool.submit([&n] { n++; });
    EXPECT_TRUE(test::waitUntil([&] { return n.load() == 100; }));
    pool.shutdown();
}

TEST(executor, scheduler_one_shot_and_periodic) {
    Scheduler s;
    std::atomic<int> once{0}, every{0};
    s.scheduleAfter(20, [&] { once++; });
    auto id = s.scheduleEvery(10, [&] { every++; });
    EXPECT_TRUE(test::waitUntil([&] { return once.load() == 1 && every.load() >= 3; }));
    EXPECT_TRUE(s.cancel(id));
    s.shutdown();
}

TEST(executor, cancel_before_due) {
    Scheduler s;
    std::atomic<int> fired{0};
    auto id = s.scheduleAfter(500, [&] { fired++; });
    EXPECT_TRUE(s.cancel(id));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(fired.load(), 0);
}

// ------------------------------------------------------------------ fileio

TEST(fileio, read_write_and_atomic) {
    test::TempDir dir;
    std::string p = dir.file("a.txt");
    EXPECT_TRUE(fs::writeFile(p, "one"));
    EXPECT_EQ(fs::readFile(p).value_or(""), "one");
    EXPECT_TRUE(fs::atomicWriteFile(p, "two"));
    EXPECT_EQ(fs::readFile(p).value_or(""), "two");
    EXPECT_FALSE(fs::exists(p + ".tmp"));
    EXPECT_EQ(fs::fileSize(p), 3);
}

TEST(fileio, file_append_read_truncate) {
    test::TempDir dir;
    fs::File f;
    ASSERT_TRUE(f.open(dir.file("data.bin"), fs::File::Mode::ReadWrite));
    EXPECT_EQ(f.append("hello"), 0);
    EXPECT_EQ(f.append(" world"), 5);
    EXPECT_EQ(f.readAt(6, 5), "world");
    EXPECT_TRUE(f.truncate(5));
    EXPECT_EQ(f.size(), 5);
    EXPECT_TRUE(f.sync());
}

TEST(fileio, directories) {
    test::TempDir dir;
    std::string sub = fs::join(dir.path(), "x/y");
    EXPECT_TRUE(fs::ensureDirectory(sub));
    fs::writeFile(fs::join(sub, "b"), "");
    fs::writeFile(fs::join(sub, "a"), "");
    auto names = fs::listDirectory(sub);
    ASSERT_EQ(names.size(), 2u);
    EXPECT_EQ(names[0], "a");
    EXPECT_TRUE(fs::removeTree(fs::join(dir.path(), "x")));
    EXPECT_FALSE(fs::exists(sub));
}

// ----------------------------------------------------------------- metrics

TEST(metrics, counters_gauges_histograms) {
    MetricsRegistry reg;
    reg.counter("tq_test_total", "test counter").inc(3);
    reg.gauge("tq_test_gauge", "test gauge").set(-2);
    auto& h = reg.histogram("tq_test_latency", "latency");
    for (int i = 0; i < 100; ++i) h.observe(i < 90 ? 40 : 4000);
    EXPECT_EQ(h.count(), 100u);
    EXPECT_EQ(h.percentile(50), 50.0);
    EXPECT_EQ(h.percentile(99), 5000.0);
    std::string text = reg.renderPrometheus();
    EXPECT_TRUE(text.find("tq_test_total 3") != std::string::npos);
    EXPECT_TRUE(text.find("tq_test_gauge -2") != std::string::npos);
    EXPECT_TRUE(text.find("tq_test_latency_count 100") != std::string::npos);
}
