// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "amqp/field_table.h"
#include "common/bytes.h"
#include "security/sasl.h"
#include "security/users.h"
#include "test.h"

using namespace tq;
using namespace tq::security;

namespace {

void fillStore(UserStore& s) {
    s.addUser("app", "app-pass", {"management"});
    s.addUser("admin", "admin-pass", {"administrator"});
    s.addUser("guest", "guest", {"administrator"});
    Permission p;
    p.user = "app";
    p.vhost = "/";
    p.configure = "^app\\.";
    p.write = "^(app\\.|amq\\.default$)";
    p.read = "^app\\.";
    s.setPermission(p);
    Permission g;
    g.user = "guest";
    g.vhost = "/";
    s.setPermission(g);
}

std::string plain(const std::string& user, const std::string& pass) {
    return std::string("\0", 1) + user + std::string("\0", 1) + pass;
}

std::string amqplain(const std::string& user, const std::string& pass) {
    ByteWriter w;
    w.shortString("LOGIN");
    w.u8('S');
    w.longString(user);
    w.shortString("PASSWORD");
    w.u8('S');
    w.longString(pass);
    return w.take();
}

}  // namespace

TEST(users, password_hashing) {
    std::string h = UserStore::hashPassword("s3cret");
    EXPECT_TRUE(UserStore::checkPassword(h, "s3cret"));
    EXPECT_FALSE(UserStore::checkPassword(h, "S3cret"));
    EXPECT_NE(h, UserStore::hashPassword("s3cret"));  // salted
    EXPECT_FALSE(UserStore::checkPassword("not-base64!", "x"));
}

TEST(users, crud_and_authenticate) {
    UserStore s;
    fillStore(s);
    EXPECT_TRUE(s.authenticate("app", "app-pass").has_value());
    EXPECT_FALSE(s.authenticate("app", "wrong").has_value());
    EXPECT_FALSE(s.authenticate("nobody", "x").has_value());
    EXPECT_FALSE(s.addUser("app", "again", {}));
    EXPECT_TRUE(s.changePassword("app", "new-pass"));
    EXPECT_TRUE(s.authenticate("app", "new-pass").has_value());
    EXPECT_TRUE(s.setTags("app", {"monitoring"}));
    EXPECT_TRUE(s.getUser("app")->hasTag("monitoring"));
    EXPECT_TRUE(s.deleteUser("app"));
    EXPECT_FALSE(s.getPermission("app", "/").has_value());
}

TEST(users, resource_permissions) {
    UserStore s;
    fillStore(s);
    EXPECT_TRUE(s.canAccessVhost("app", "/"));
    EXPECT_FALSE(s.canAccessVhost("app", "other"));
    EXPECT_TRUE(s.checkResource("app", "/", Access::Configure, "app.orders"));
    EXPECT_FALSE(s.checkResource("app", "/", Access::Configure, "billing"));
    EXPECT_TRUE(s.checkResource("app", "/", Access::Write, "amq.default"));
    EXPECT_FALSE(s.checkResource("app", "/", Access::Write, "amq.topic"));
    EXPECT_TRUE(s.checkResource("guest", "/", Access::Read, "anything"));
    EXPECT_FALSE(s.checkResource("app", "other", Access::Read, "app.x"));
}

TEST(users, topic_permissions) {
    UserStore s;
    fillStore(s);
    TopicPermission tp;
    tp.user = "app";
    tp.vhost = "/";
    tp.exchange = "amq.topic";
    tp.write = "^devices\\.{username}\\.";
    tp.read = ".*";
    s.setTopicPermission(tp);
    EXPECT_TRUE(s.checkTopic("app", "/", "amq.topic", "devices.app.temp", true));
    EXPECT_FALSE(s.checkTopic("app", "/", "amq.topic", "devices.other.temp", true));
    EXPECT_TRUE(s.checkTopic("app", "/", "logs", "anything", true));
    EXPECT_TRUE(s.clearTopicPermission("app", "/", "amq.topic"));
    EXPECT_TRUE(s.checkTopic("app", "/", "amq.topic", "devices.other.temp", true));
}

TEST(users, json_round_trip) {
    UserStore s;
    fillStore(s);
    UserStore copy;
    copy.loadJson(s.toJson());
    EXPECT_EQ(copy.userCount(), 3u);
    EXPECT_TRUE(copy.authenticate("admin", "admin-pass").has_value());
    EXPECT_TRUE(copy.checkResource("app", "/", Access::Read, "app.q"));
    EXPECT_EQ(copy.listPermissions("/").size(), 2u);
}

TEST(sasl, plain_success_and_failure) {
    UserStore s;
    fillStore(s);
    Authenticator auth(s, AuthenticatorOptions{});
    auto ok = auth.authenticate("PLAIN", plain("app", "app-pass"), "10.1.2.3");
    EXPECT_TRUE(ok.ok);
    EXPECT_EQ(ok.user, "app");
    EXPECT_FALSE(auth.authenticate("PLAIN", plain("app", "nope"), "10.1.2.3").ok);
    EXPECT_FALSE(auth.authenticate("PLAIN", "garbage", "10.1.2.3").ok);
    EXPECT_FALSE(auth.authenticate("EXTERNAL", "", "10.1.2.3").ok);
    EXPECT_EQ(auth.mechanismsString(), "PLAIN AMQPLAIN");
}

TEST(sasl, guest_is_loopback_only_with_plain) {
    UserStore s;
    fillStore(s);
    Authenticator auth(s, AuthenticatorOptions{});
    EXPECT_TRUE(auth.authenticate("PLAIN", plain("guest", "guest"), "127.0.0.1").ok);
    EXPECT_TRUE(auth.authenticate("PLAIN", plain("guest", "guest"), "::1").ok);
    auto remote = auth.authenticate("PLAIN", plain("guest", "guest"), "192.168.1.10");
    EXPECT_FALSE(remote.ok);
    EXPECT_TRUE(remote.error.find("localhost") != std::string::npos);
    EXPECT_FALSE(auth.authenticatePassword("guest", "guest", "192.168.1.10").ok);
}

TEST(sasl, amqplain) {
    UserStore s;
    fillStore(s);
    Authenticator auth(s, AuthenticatorOptions{});
    auto r = auth.authenticate("AMQPLAIN", amqplain("admin", "admin-pass"), "127.0.0.1");
    EXPECT_TRUE(r.ok);
    EXPECT_EQ(r.user, "admin");
    EXPECT_FALSE(auth.authenticate("AMQPLAIN", amqplain("admin", "x"), "127.0.0.1").ok);
    EXPECT_FALSE(auth.authenticate("AMQPLAIN", "\x05", "127.0.0.1").ok);
}
