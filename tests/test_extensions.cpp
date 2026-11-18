// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Unit tests for access tokens, runtime parameters, the configuration schema,
// shovel and federation definitions.
#include "broker/feature_flags.h"
#include "broker/parameters.h"
#include "common/clock.h"
#include "common/config.h"
#include "common/config_schema.h"
#include "federation/federation.h"
#include "security/jwt.h"
#include "security/users.h"
#include "shovel/shovel.h"
#include "test.h"

using namespace tq;

namespace {

security::JwtConfig jwtConfig() {
    security::JwtConfig c;
    c.enabled = true;
    c.resourceServerId = "tropiqueue";
    c.defaultKey = "unit-test-signing-key";
    c.keys["k2"] = "second-signing-key";
    c.leewaySeconds = 0;
    return c;
}

Json header(const std::string& kid = "") {
    Json h = Json::object();
    h["alg"] = "HS256";
    h["typ"] = "JWT";
    if (!kid.empty()) h["kid"] = kid;
    return h;
}

Json claims(const std::string& sub, const std::string& scope, int64_t expiresIn = 600) {
    Json p = Json::object();
    p["sub"] = sub;
    p["scope"] = scope;
    p["aud"] = "tropiqueue";
    p["exp"] = wallClockMs() / 1000 + expiresIn;
    return p;
}

}  // namespace

// --------------------------------------------------------------------- jwt

TEST(jwt, base64url_round_trip) {
    std::string data("\xfb\xff\x00 binary?", 11);
    std::string enc = security::base64UrlEncode(data);
    EXPECT_EQ(enc.find('+'), std::string::npos);
    EXPECT_EQ(enc.find('/'), std::string::npos);
    EXPECT_EQ(enc.find('='), std::string::npos);
    auto dec = security::base64UrlDecode(enc);
    ASSERT_TRUE(dec.has_value());
    EXPECT_EQ(*dec, data);
}

TEST(jwt, wildcard_match) {
    EXPECT_TRUE(security::wildcardMatch("*", "anything"));
    EXPECT_TRUE(security::wildcardMatch("orders.*", "orders.eu"));
    EXPECT_TRUE(security::wildcardMatch("*-dlq", "billing-dlq"));
    EXPECT_TRUE(security::wildcardMatch("a*c*e", "abcde"));
    EXPECT_FALSE(security::wildcardMatch("orders.*", "order.eu"));
    EXPECT_FALSE(security::wildcardMatch("abc", "abcd"));
    EXPECT_TRUE(security::wildcardMatch("", ""));
    EXPECT_FALSE(security::wildcardMatch("", "x"));
}

TEST(jwt, valid_token_grants_scopes) {
    security::JwtValidator v(jwtConfig());
    std::string token = security::JwtValidator::sign(
        header(), claims("svc-orders", "tropiqueue.read:prod/orders.* tropiqueue.write:prod/* tropiqueue.tag:monitoring other.read:*/*"),
        "unit-test-signing-key");
    EXPECT_TRUE(security::JwtValidator::looksLikeJwt(token));
    std::string err;
    auto id = v.validate(token, &err);
    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(id->user, "svc-orders");
    EXPECT_EQ(id->grants.size(), 2u);
    EXPECT_TRUE(id->tags.count("monitoring") == 1);
    EXPECT_EQ(id->grants[0].vhostPattern, "prod");
    EXPECT_EQ(id->grants[0].resourcePattern, "orders.*");
}

TEST(jwt, key_id_selects_key) {
    security::JwtValidator v(jwtConfig());
    std::string good = security::JwtValidator::sign(header("k2"), claims("u", "tropiqueue.read:*/*"), "second-signing-key");
    std::string err;
    EXPECT_TRUE(v.validate(good, &err).has_value());
    std::string unknown = security::JwtValidator::sign(header("k9"), claims("u", "tropiqueue.read:*/*"), "whatever");
    EXPECT_FALSE(v.validate(unknown, &err).has_value());
    EXPECT_NE(err.find("unknown signing key"), std::string::npos);
}

TEST(jwt, rejects_bad_signature_expiry_and_audience) {
    security::JwtValidator v(jwtConfig());
    std::string err;
    std::string forged = security::JwtValidator::sign(header(), claims("u", "tropiqueue.read:*/*"), "wrong-key");
    EXPECT_FALSE(v.validate(forged, &err).has_value());
    EXPECT_NE(err.find("signature"), std::string::npos);

    std::string expired = security::JwtValidator::sign(header(), claims("u", "tropiqueue.read:*/*", -120),
                                                       "unit-test-signing-key");
    EXPECT_FALSE(v.validate(expired, &err).has_value());
    EXPECT_NE(err.find("expired"), std::string::npos);

    Json p = claims("u", "tropiqueue.read:*/*");
    p["aud"] = "another-service";
    std::string wrongAud = security::JwtValidator::sign(header(), p, "unit-test-signing-key");
    EXPECT_FALSE(v.validate(wrongAud, &err).has_value());
    EXPECT_NE(err.find("audience"), std::string::npos);

    Json noSub = claims("", "tropiqueue.read:*/*");
    std::string anonymous = security::JwtValidator::sign(header(), noSub, "unit-test-signing-key");
    EXPECT_FALSE(v.validate(anonymous, &err).has_value());
}

TEST(jwt, rejects_malformed_tokens) {
    security::JwtValidator v(jwtConfig());
    std::string err;
    EXPECT_FALSE(v.validate("not-a-token", &err).has_value());
    EXPECT_FALSE(v.validate("a.b.c", &err).has_value());
    EXPECT_FALSE(security::JwtValidator::looksLikeJwt("guest"));
    EXPECT_FALSE(security::JwtValidator::looksLikeJwt("eyJhbGciOiJIUzI1NiJ9"));

    security::JwtConfig off = jwtConfig();
    off.enabled = false;
    security::JwtValidator disabled(off);
    std::string token = security::JwtValidator::sign(header(), claims("u", "tropiqueue.read:*/*"), "unit-test-signing-key");
    EXPECT_FALSE(disabled.validate(token, &err).has_value());
}

TEST(jwt, token_sessions_drive_permissions) {
    security::UserStore users;
    security::TokenSession s;
    s.user = "svc";
    s.tags = {"management"};
    s.grants.push_back({security::Access::Read, "prod", "orders.*"});
    s.grants.push_back({security::Access::Configure, "*", "tmp-*"});
    users.putTokenSession(s);
    EXPECT_TRUE(users.canAccessVhost("svc", "prod"));
    EXPECT_TRUE(users.canAccessVhost("svc", "dev"));
    EXPECT_TRUE(users.checkResource("svc", "prod", security::Access::Read, "orders.eu"));
    EXPECT_FALSE(users.checkResource("svc", "prod", security::Access::Write, "orders.eu"));
    EXPECT_TRUE(users.checkResource("svc", "dev", security::Access::Configure, "tmp-123"));
    auto u = users.getUser("svc");
    ASSERT_TRUE(u.has_value());
    EXPECT_TRUE(u->hasTag("management"));

    users.putTokenSession(s);  // second connection with the same identity
    users.dropTokenSession("svc");
    EXPECT_EQ(users.tokenSessionCount(), 1u);
    users.dropTokenSession("svc");
    EXPECT_EQ(users.tokenSessionCount(), 0u);
    EXPECT_FALSE(users.canAccessVhost("svc", "prod"));
}

// --------------------------------------------------------------- parameters

TEST(parameters, set_get_list_clear) {
    broker::ParameterStore store;
    store.registerComponent("widget", nullptr);
    broker::RuntimeParameter p{"widget", "/", "w1", Json::parse(R"({"size": 3})")};
    std::string err;
    ASSERT_TRUE(store.set(p, &err));
    auto got = store.get("widget", "/", "w1");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->value["size"].asInt(), 3);
    store.set(broker::RuntimeParameter{"widget", "other", "w2", Json::parse("{}")}, &err);
    EXPECT_EQ(store.list().size(), 2u);
    EXPECT_EQ(store.list("widget", "/").size(), 1u);
    EXPECT_TRUE(store.clear("widget", "/", "w1"));
    EXPECT_FALSE(store.clear("widget", "/", "w1"));
    store.clearVhost("other");
    EXPECT_TRUE(store.list().empty());
}

TEST(parameters, unknown_component_and_validation) {
    broker::ParameterStore store;
    std::string err;
    EXPECT_FALSE(store.set(broker::RuntimeParameter{"nope", "/", "x", Json::object()}, &err));
    EXPECT_NE(err.find("unknown"), std::string::npos);
    store.registerComponent("strict", [](const std::string&, const std::string&, const Json& v, std::string* e) {
        if (!v.contains("required")) {
            *e = "missing required";
            return false;
        }
        return true;
    });
    EXPECT_FALSE(store.set(broker::RuntimeParameter{"strict", "/", "x", Json::object()}, &err));
    EXPECT_EQ(err, "missing required");
    EXPECT_TRUE(store.set(broker::RuntimeParameter{"strict", "/", "x", Json::parse(R"({"required": 1})")}, &err));
}

TEST(parameters, listeners_see_changes) {
    broker::ParameterStore store;
    store.registerComponent("c", nullptr);
    int sets = 0, removes = 0;
    store.addListener("c", [&](const broker::RuntimeParameter&, bool removed) { (removed ? removes : sets)++; });
    std::string err;
    store.set(broker::RuntimeParameter{"c", "/", "a", Json::object()}, &err);
    store.set(broker::RuntimeParameter{"c", "/", "a", Json::object()}, &err);
    store.clear("c", "/", "a");
    EXPECT_EQ(sets, 2);
    EXPECT_EQ(removes, 1);
}

TEST(parameters, globals_and_cluster_name) {
    broker::ParameterStore store;
    EXPECT_EQ(store.clusterName("node-a"), "node-a");
    store.setGlobal("cluster_name", Json("east"));
    EXPECT_EQ(store.clusterName("node-a"), "east");
    EXPECT_EQ(store.globals().size(), 1u);
    EXPECT_TRUE(store.clearGlobal("cluster_name"));
    EXPECT_FALSE(store.global("cluster_name").has_value());
}

TEST(parameters, json_round_trip) {
    broker::RuntimeParameter p{"shovel", "/", "s", Json::parse(R"({"a": 1})")};
    std::string err;
    auto back = broker::RuntimeParameter::fromJson(p.toJson(), &err);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->name, "s");
    EXPECT_EQ(back->value["a"].asInt(), 1);
    EXPECT_FALSE(broker::RuntimeParameter::fromJson(Json::parse(R"({"component": "x"})"), &err).has_value());
}

// ------------------------------------------------------------ config schema

TEST(config_schema, edit_distance) {
    EXPECT_EQ(editDistance("", ""), 0u);
    EXPECT_EQ(editDistance("heartbeat", "heartbeat"), 0u);
    EXPECT_EQ(editDistance("heartbeat", "hearbeat"), 1u);
    EXPECT_EQ(editDistance("kitten", "sitting"), 3u);
}

TEST(config_schema, known_keys_validate) {
    auto cfg = Config::fromString(
        "listeners.tcp.default = 127.0.0.1:5673\n"
        "heartbeat = 30\n"
        "vm_memory_high_watermark.relative = 0.6\n"
        "frame_max = 128KB\n"
        "consumer_timeout = 15m\n"
        "log.level = debug\n"
        "auth_oauth2.signing_keys.k1 = secret\n"
        "management.tcp.port = 15673\n");
    EXPECT_TRUE(ConfigSchema::broker().validate(cfg).empty());
}

TEST(config_schema, reports_unknown_keys_with_hints) {
    auto cfg = Config::fromString("heartbaet = 30\nsomething.else = 1\n");
    auto issues = ConfigSchema::broker().validate(cfg);
    ASSERT_EQ(issues.size(), 2u);
    for (const auto& i : issues) EXPECT_TRUE(i.level == ConfigIssue::Level::Warning);
    bool hinted = false;
    for (const auto& i : issues) hinted = hinted || i.message.find("did you mean 'heartbeat'") != std::string::npos;
    EXPECT_TRUE(hinted);
}

TEST(config_schema, reports_bad_values) {
    auto cfg = Config::fromString(
        "heartbeat = soon\n"
        "vm_memory_high_watermark.relative = 1.5\n"
        "management.tcp.port = 70000\n"
        "listeners.tcp.default = host:\n"
        "consumer_timeout = forever\n"
        "cluster.cookie = 12 3\n");
    auto issues = ConfigSchema::broker().validate(cfg);
    int errors = 0;
    for (const auto& i : issues) errors += i.level == ConfigIssue::Level::Error ? 1 : 0;
    EXPECT_EQ(errors, 5);
}

TEST(config_schema, check_value_types) {
    ConfigKey endpoint;
    endpoint.type = ConfigType::Endpoint;
    EXPECT_EQ(ConfigSchema::checkValue(endpoint, "5672"), "");
    EXPECT_EQ(ConfigSchema::checkValue(endpoint, "[::1]:5672"), "");
    EXPECT_NE(ConfigSchema::checkValue(endpoint, "[::1"), "");
    ConfigKey b;
    b.type = ConfigType::Bool;
    EXPECT_EQ(ConfigSchema::checkValue(b, "yes"), "");
    EXPECT_NE(ConfigSchema::checkValue(b, "maybe"), "");
    ConfigKey e;
    e.type = ConfigType::Enum;
    e.choices = {"a", "b"};
    EXPECT_EQ(ConfigSchema::checkValue(e, "b"), "");
    EXPECT_NE(ConfigSchema::checkValue(e, "c"), "");
}

TEST(config_schema, reference_and_sample) {
    std::string ref = ConfigSchema::broker().reference();
    EXPECT_NE(ref.find("`listeners.tcp.default`"), std::string::npos);
    std::string sample = ConfigSchema::broker().sampleConfig();
    EXPECT_NE(sample.find("# heartbeat = 60"), std::string::npos);
    EXPECT_EQ(sample.find("guest\n# default_vhost"), std::string::npos);
    auto parsed = Config::fromString(sample);
    EXPECT_TRUE(parsed.all().empty());
}

// ------------------------------------------------------------------- shovel

TEST(shovel, redact_uri) {
    EXPECT_EQ(shovel::redactUri("amqp://user:pass@host:5672/%2F"), "amqp://host:5672/%2F");
    EXPECT_EQ(shovel::redactUri("amqp://host/vh"), "amqp://host/vh");
    EXPECT_EQ(shovel::redactUri("amqp://u@h"), "amqp://h");
    EXPECT_EQ(shovel::redactUri("not a uri"), "not a uri");
}

TEST(shovel, definition_parsing) {
    std::string err;
    auto d = shovel::ShovelDef::fromJson("/", "s1", Json::parse(R"({
        "src-uri": "amqp://u:p@localhost/%2F", "src-queue": "a",
        "dest-uri": ["amqp://u:p@remote/%2F"], "dest-exchange": "x", "dest-exchange-key": "k",
        "ack-mode": "on-publish", "src-delete-after": 10, "reconnect-delay": 2})"),
                                         &err);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->srcQueue, "a");
    EXPECT_EQ(d->destUri, "amqp://u:p@remote/%2F");
    EXPECT_TRUE(d->ackMode == shovel::AckMode::OnPublish);
    EXPECT_EQ(d->srcDeleteAfter, "10");
    EXPECT_EQ(d->reconnectDelaySec, 2);
    ASSERT_TRUE(d->destExchangeKey.has_value());
    auto again = shovel::ShovelDef::fromJson("/", "s1", d->toJson(), &err);
    ASSERT_TRUE(again.has_value());
    EXPECT_EQ(again->destExchange, "x");
}

TEST(shovel, definition_errors) {
    std::string err;
    auto bad = [&](const char* json) { return !shovel::ShovelDef::fromJson("/", "s", Json::parse(json), &err); };
    EXPECT_TRUE(bad(R"({"dest-uri": "amqp://h", "src-queue": "a"})"));
    EXPECT_TRUE(bad(R"({"src-uri": "amqp://h", "dest-uri": "amqp://h"})"));
    EXPECT_TRUE(bad(R"({"src-uri": "amqp://h", "dest-uri": "amqp://h", "src-queue": "a", "src-exchange": "x"})"));
    EXPECT_TRUE(bad(R"({"src-uri": "amqp://h", "dest-uri": "amqp://h", "src-queue": "a", "ack-mode": "sometimes"})"));
    EXPECT_TRUE(bad(R"({"src-uri": "amqp://h", "dest-uri": "amqp://h", "src-exchange": "x", "src-delete-after": "queue-length"})"));
    EXPECT_TRUE(bad(R"({"src-uri": "http://h", "dest-uri": "amqp://h", "src-queue": "a"})"));
    EXPECT_TRUE(bad(R"({"src-uri": "amqp://h", "dest-uri": "amqp://h", "src-queue": "a", "src-prefetch-count": 0})"));
}

// --------------------------------------------------------------- federation

TEST(federation, upstream_parsing) {
    std::string err;
    auto u = federation::Upstream::fromJson("/", "dc1", Json::parse(R"({
        "uri": "amqp://u:p@dc1/%2F", "max-hops": 2, "prefetch-count": 50, "expires": 60000, "exchange": "events"})"),
                                            &err);
    ASSERT_TRUE(u.has_value());
    EXPECT_EQ(u->maxHops, 2);
    EXPECT_EQ(u->prefetchCount, 50);
    EXPECT_EQ(u->expiresMs, 60000);
    EXPECT_EQ(u->exchange.value_or(""), "events");
    EXPECT_FALSE(federation::Upstream::fromJson("/", "x", Json::parse(R"({"max-hops": 1})"), &err).has_value());
    EXPECT_FALSE(
        federation::Upstream::fromJson("/", "x", Json::parse(R"({"uri": "amqp://h", "max-hops": 0})"), &err).has_value());
}

TEST(federation, received_from_header) {
    EXPECT_EQ(federation::receivedFromCount(nullptr), 0u);
    amqp::FieldTable h;
    h.set("app", amqp::FieldValue::longString("billing"));
    auto once = federation::appendReceivedFrom(&h, "amqp://dc1", "events", false, "dc1");
    EXPECT_EQ(federation::receivedFromCount(&once), 1u);
    EXPECT_TRUE(federation::receivedFromContains(&once, "dc1"));
    EXPECT_FALSE(federation::receivedFromContains(&once, "dc2"));
    EXPECT_EQ(once.getString("app").value_or(""), "billing");
    auto twice = federation::appendReceivedFrom(&once, "amqp://dc2", "events", true, "dc2");
    EXPECT_EQ(federation::receivedFromCount(&twice), 2u);
    EXPECT_TRUE(federation::receivedFromContains(&twice, "dc2"));
}

// ------------------------------------------------------------ feature flags

TEST(feature_flags, fresh_and_upgraded_nodes) {
    broker::FeatureFlags fresh;
    fresh.initFresh();
    EXPECT_TRUE(fresh.isEnabled("quorum_queue"));
    EXPECT_TRUE(fresh.isEnabled("user_limits"));
    EXPECT_FALSE(fresh.isEnabled("detailed_queue_metrics"));

    broker::FeatureFlags upgraded;
    upgraded.initUpgraded();
    EXPECT_TRUE(upgraded.isEnabled("stream_queue"));
    EXPECT_FALSE(upgraded.isEnabled("user_limits"));
}

TEST(feature_flags, enable_pulls_in_dependencies) {
    broker::FeatureFlags flags;
    flags.initUpgraded();
    std::string err;
    ASSERT_TRUE(flags.enable("detailed_queue_metrics", {}, &err));
    EXPECT_TRUE(flags.isEnabled("virtual_host_metadata"));
    EXPECT_FALSE(flags.enable("no_such_flag", {}, &err));
    EXPECT_NE(err.find("unknown"), std::string::npos);
    EXPECT_FALSE(flags.enable("user_limits", {"quorum_queue"}, &err));
    EXPECT_NE(err.find("not supported"), std::string::npos);
    EXPECT_FALSE(flags.isEnabled("user_limits"));
}

TEST(feature_flags, json_and_load) {
    broker::FeatureFlags flags;
    flags.load(Json::parse(R"(["user_limits", "bogus"])"));
    EXPECT_TRUE(flags.isEnabled("user_limits"));
    EXPECT_TRUE(flags.isEnabled("quorum_queue"));
    EXPECT_FALSE(flags.isEnabled("bogus"));
    Json j = flags.toJson();
    ASSERT_EQ(j.size(), broker::FeatureFlags::known().size());
    bool sawExperimental = false;
    for (const auto& f : j.asArray()) sawExperimental = sawExperimental || f["stability"].asString() == "experimental";
    EXPECT_TRUE(sawExperimental);
}
