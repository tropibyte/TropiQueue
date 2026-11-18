// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "mgmt/routes.h"

#include <algorithm>
#include <filesystem>

#include "amqp/constants.h"
#include "broker/broker.h"
#include "broker/channel.h"
#include "broker/connection.h"
#include "broker/vhost.h"
#include "cluster/cluster.h"
#include "common/clock.h"
#include "common/crypto.h"
#include "common/fileio.h"
#include "common/logging.h"
#include "common/metrics.h"
#include "common/platform.h"
#include "common/strings.h"
#include "mgmt/route_util.h"

namespace tq::mgmt {

using broker::Broker;
using net::HttpRequest;
using net::HttpResponse;
using net::PathParams;
using security::Access;

namespace {
thread_local RequestContext* g_ctx = nullptr;
}  // namespace

namespace util {

RequestContext& ctx() {
    if (!g_ctx) throw RouteError(500, "internal_error", "no request context");
    return *g_ctx;
}

[[noreturn]] void forbidden(const std::string& reason) {
    throw RouteError(403, "access_refused", reason);
}

[[noreturn]] void notFound(const std::string& reason) {
    throw RouteError(404, "not_found", reason);
}

[[noreturn]] void badRequest(const std::string& reason) { throw RouteError(400, "bad_request", reason); }

void requireAdmin() {
    if (!ctx().isAdmin()) forbidden("Not administrator user");
}

void requireTag(const std::string& tag) {
    if (!ctx().hasTag(tag)) forbidden("Not " + tag + " user");
}

Json body(const HttpRequest& req) {
    if (str::trim(req.body).empty()) return Json::object();
    Json j = Json::parse(req.body);
    if (!j.isObject()) badRequest("request body must be a JSON object");
    return j;
}

broker::VhostPtr vhostOr404(Broker& b, const std::string& name) {
    auto vh = b.vhost(name);
    if (!vh) notFound("vhost '" + name + "' not found");
    return vh;
}

bool canSeeVhost(Broker& b, const std::string& vhost) {
    return ctx().isAdmin() || ctx().hasTag("monitoring") || b.users().canAccessVhost(ctx().user.name, vhost);
}

void requireVhostAccess(Broker& b, const std::string& vhost) {
    if (!canSeeVhost(b, vhost)) forbidden("Access refused to vhost '" + vhost + "'");
}

void requirePermission(Broker& b, const std::string& vhost, Access access, const std::string& resource) {
    if (ctx().isAdmin()) return;
    if (!b.users().checkResource(ctx().user.name, vhost, access, resource)) {
        forbidden("access to '" + resource + "' in vhost '" + vhost + "' refused for user '" + ctx().user.name + "'");
    }
}

HttpResponse fromOp(const broker::OpResult& r) {
    if (r.ok()) return HttpResponse::noContent();
    int status = 400;
    if (r.code == amqp::kNotFound) status = 404;
    else if (r.code == amqp::kAccessRefused) status = 403;
    else if (r.code == amqp::kResourceLocked) status = 409;
    else if (r.code == amqp::kInternalError) status = 500;
    return HttpResponse::error(status, std::string(amqp::replyCodeName(r.code)), r.text);
}

HttpResponse fromCommit(const Json& r) { return fromOp(broker::OpResult::fromJson(r)); }

}  // namespace util

namespace {

using namespace util;

Json queueJson(const broker::QueuePtr& q) {
    Json j = q->info();
    j["node"] = q->def().node;
    j["state"] = "running";
    return j;
}

Json exchangeJson(const broker::ExchangeDef& d) {
    Json j = d.toJson();
    if (j["name"].asString().empty()) j["name"] = "";
    return j;
}

std::string decodePayload(const Json& b) {
    std::string payload = b["payload"].asString();
    if (b["payload_encoding"].asString("string") == "base64") {
        auto raw = crypto::base64Decode(payload);
        if (!raw) badRequest("payload is not valid base64");
        return *raw;
    }
    return payload;
}

// --------------------------------------------------------------- overview

HttpResponse getOverview(Broker& b, const HttpRequest&, const PathParams&) {
    Json j = b.overview();
    j["user"] = ctx().user.name;
    return HttpResponse::json(200, j);
}

HttpResponse getNodes(Broker& b, const HttpRequest&, const PathParams&) {
    requireTag("monitoring");
    Json arr = Json::array();
    Json n = Json::object();
    n["name"] = b.nodeName();
    n["id"] = b.config().nodeId;
    n["running"] = b.running();
    n["uptime"] = wallClockMs() - b.startedAtMs();
    n["mem_alarm"] = b.memoryAlarm();
    n["mem_limit"] = b.memoryLimit();
    n["mem_used"] = static_cast<int64_t>(b.messageMemory());
    n["os"] = kPlatformName;
    arr.push(n);
    return HttpResponse::json(200, arr);
}

HttpResponse getCluster(Broker& b, const HttpRequest&, const PathParams&) {
    requireTag("monitoring");
    return HttpResponse::json(200, b.cluster() ? b.cluster()->status() : Json::object());
}

// ------------------------------------------------------------- connections

HttpResponse getConnections(Broker& b, const HttpRequest& req, const PathParams&) {
    Json arr = Json::array();
    bool all = ctx().hasTag("monitoring");
    for (const auto& c : b.connections()) {
        if (!all && c->user() != ctx().user.name) continue;
        arr.push(c->info());
    }
    return HttpResponse::json(200, applyListQuery(req, arr));
}

std::shared_ptr<broker::AmqpConnection> findConnection(Broker& b, const std::string& name) {
    for (const auto& c : b.connections()) {
        if (c->name() == name) return c;
    }
    return nullptr;
}

HttpResponse getConnection(Broker& b, const HttpRequest&, const PathParams& p) {
    auto c = findConnection(b, p.at("name"));
    if (!c) notFound();
    if (!ctx().hasTag("monitoring") && c->user() != ctx().user.name) notFound();
    return HttpResponse::json(200, c->info());
}

HttpResponse deleteConnection(Broker& b, const HttpRequest& req, const PathParams& p) {
    auto c = findConnection(b, p.at("name"));
    if (!c) notFound();
    if (!ctx().hasTag("monitoring") && c->user() != ctx().user.name) forbidden();
    std::string reason = req.header("x-reason", "Closed via management plugin");
    c->closeWithError(amqp::kConnectionForced, "CONNECTION_FORCED - " + reason);
    return HttpResponse::noContent();
}

HttpResponse getChannels(Broker& b, const HttpRequest&, const PathParams&) {
    Json arr = Json::array();
    bool all = ctx().hasTag("monitoring");
    for (const auto& c : b.connections()) {
        if (!all && c->user() != ctx().user.name) continue;
        Json cj = Json::object();
        cj["connection"] = c->name();
        cj["user"] = c->user();
        cj["vhost"] = c->vhostName();
        cj["channels"] = static_cast<int64_t>(c->channelCount());
        arr.push(cj);
    }
    return HttpResponse::json(200, arr);
}

// ------------------------------------------------------------------ vhosts

Json vhostJson(const broker::VhostPtr& vh) {
    Json j = Json::object();
    j["name"] = vh->name();
    int64_t messages = 0, ready = 0, unacked = 0;
    for (const auto& q : vh->queues()) {
        ready += static_cast<int64_t>(q->messageCount());
        unacked += static_cast<int64_t>(q->unackedCount());
    }
    messages = ready + unacked;
    j["messages"] = messages;
    j["messages_ready"] = ready;
    j["messages_unacknowledged"] = unacked;
    j["queues"] = static_cast<int64_t>(vh->queueCount());
    j["connections"] = vh->connectionCount().load();
    j["limits"] = vh->limits().toJson();
    j["tracing"] = vh->tracing();
    return j;
}

HttpResponse getVhosts(Broker& b, const HttpRequest&, const PathParams&) {
    Json arr = Json::array();
    for (const auto& vh : b.vhosts()) {
        if (canSeeVhost(b, vh->name())) arr.push(vhostJson(vh));
    }
    return HttpResponse::json(200, arr);
}

HttpResponse getVhost(Broker& b, const HttpRequest&, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requireVhostAccess(b, vh->name());
    return HttpResponse::json(200, vhostJson(vh));
}

HttpResponse putVhost(Broker& b, const HttpRequest&, const PathParams& p) {
    requireAdmin();
    Json cmd = Json::object();
    cmd["op"] = "add_vhost";
    cmd["name"] = p.at("vhost");
    return fromCommit(b.commitMetadata(cmd));
}

HttpResponse deleteVhost(Broker& b, const HttpRequest&, const PathParams& p) {
    requireAdmin();
    Json cmd = Json::object();
    cmd["op"] = "delete_vhost";
    cmd["name"] = p.at("vhost");
    return fromCommit(b.commitMetadata(cmd));
}

HttpResponse getVhostLimits(Broker& b, const HttpRequest&, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requireVhostAccess(b, vh->name());
    Json j = Json::object();
    j["vhost"] = vh->name();
    j["value"] = vh->limits().toJson();
    return HttpResponse::json(200, j);
}

HttpResponse putVhostLimit(Broker& b, const HttpRequest& req, const PathParams& p) {
    requireAdmin();
    auto vh = vhostOr404(b, p.at("vhost"));
    Json bj = body(req);
    Json limits = vh->limits().toJson();
    const std::string& name = p.at("name");
    if (name != "max-connections" && name != "max-queues") badRequest("unknown limit '" + name + "'");
    limits[name] = bj["value"].asInt(-1);
    Json cmd = Json::object();
    cmd["op"] = "set_vhost_limits";
    cmd["vhost"] = vh->name();
    cmd["limits"] = limits;
    return fromCommit(b.commitMetadata(cmd));
}

// --------------------------------------------------------------- exchanges

HttpResponse getExchanges(Broker& b, const HttpRequest& req, const PathParams& p) {
    Json arr = Json::array();
    for (const auto& vh : b.vhosts()) {
        if (p.count("vhost") && vh->name() != p.at("vhost")) continue;
        if (!canSeeVhost(b, vh->name())) continue;
        for (const auto& d : vh->exchanges()) arr.push(exchangeJson(d));
    }
    if (p.count("vhost")) vhostOr404(b, p.at("vhost"));
    return HttpResponse::json(200, applyListQuery(req, arr));
}

HttpResponse getExchange(Broker& b, const HttpRequest&, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requireVhostAccess(b, vh->name());
    auto d = vh->exchangeDef(p.at("name"));
    if (!d) notFound();
    return HttpResponse::json(200, exchangeJson(*d));
}

HttpResponse putExchange(Broker& b, const HttpRequest& req, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requirePermission(b, vh->name(), Access::Configure, p.at("name"));
    Json bj = body(req);
    broker::ExchangeDef d;
    d.name = p.at("name");
    d.vhost = vh->name();
    auto type = broker::parseExchangeType(bj["type"].asString("direct"));
    if (!type) badRequest("invalid exchange type '" + bj["type"].asString() + "'");
    d.type = *type;
    d.durable = bj["durable"].asBool(true);
    d.autoDelete = bj["auto_delete"].asBool(false);
    d.internal = bj["internal"].asBool(false);
    d.arguments = amqp::FieldTable::fromJson(bj["arguments"]);
    if (d.durable) {
        Json cmd = Json::object();
        cmd["op"] = "declare_exchange";
        cmd["def"] = d.toJson();
        return fromCommit(b.commitMetadata(cmd));
    }
    return fromOp(vh->declareExchange(d, false));
}

HttpResponse deleteExchange(Broker& b, const HttpRequest& req, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requirePermission(b, vh->name(), Access::Configure, p.at("name"));
    auto d = vh->exchangeDef(p.at("name"));
    if (!d) notFound();
    bool ifUnused = req.queryParam("if-unused") == "true";
    if (d->durable) {
        Json cmd = Json::object();
        cmd["op"] = "delete_exchange";
        cmd["vhost"] = vh->name();
        cmd["name"] = d->name;
        cmd["if_unused"] = ifUnused;
        return fromCommit(b.commitMetadata(cmd));
    }
    return fromOp(vh->deleteExchange(d->name, ifUnused));
}

HttpResponse publishToExchange(Broker& b, const HttpRequest& req, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    std::string exchange = p.at("name") == "amq.default" ? "" : p.at("name");
    requirePermission(b, vh->name(), Access::Write, exchange.empty() ? "amq.default" : exchange);
    if (!vh->exchangeDef(exchange)) notFound();
    Json bj = body(req);
    auto msg = std::make_shared<broker::Message>();
    msg->id = broker::nextMessageId();
    msg->exchange = exchange;
    msg->routingKey = bj["routing_key"].asString();
    msg->props = amqp::BasicProperties::fromJson(bj["properties"]);
    msg->body = decodePayload(bj);
    msg->publishedAtMs = wallClockMs();
    msg->publisherVhost = vh->name();
    bool routed = false;
    vh->publish(exchange, msg, &routed);
    Json j = Json::object();
    j["routed"] = routed;
    return HttpResponse::json(200, j);
}

// ------------------------------------------------------------------ queues

HttpResponse getQueues(Broker& b, const HttpRequest& req, const PathParams& p) {
    Json arr = Json::array();
    if (p.count("vhost")) {
        auto vh = vhostOr404(b, p.at("vhost"));
        requireVhostAccess(b, vh->name());
        for (const auto& q : vh->queues()) arr.push(queueJson(q));
        return HttpResponse::json(200, applyListQuery(req, arr));
    }
    for (const auto& vh : b.vhosts()) {
        for (const auto& q : vh->queues()) arr.push(queueJson(q));
    }
    return HttpResponse::json(200, applyListQuery(req, arr));
}

HttpResponse getQueue(Broker& b, const HttpRequest&, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requireVhostAccess(b, vh->name());
    auto q = vh->findQueue(p.at("name"));
    if (!q) notFound();
    return HttpResponse::json(200, queueJson(q));
}

HttpResponse putQueue(Broker& b, const HttpRequest& req, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requirePermission(b, vh->name(), Access::Configure, p.at("name"));
    Json bj = body(req);
    broker::QueueDef d;
    d.name = p.at("name");
    d.vhost = vh->name();
    d.durable = bj["durable"].asBool(true);
    d.autoDelete = bj["auto_delete"].asBool(false);
    d.arguments = amqp::FieldTable::fromJson(bj["arguments"]);
    auto type = broker::parseQueueType(d.arguments.getString("x-queue-type").value_or(bj["type"].asString("classic")));
    if (!type) badRequest("invalid queue type");
    d.type = *type;
    if (d.type != broker::QueueType::Classic && !d.durable) badRequest("quorum queues and streams must be durable");
    if (vh->findQueue(d.name)) return HttpResponse::noContent();
    if (d.durable) {
        Json cmd = Json::object();
        cmd["op"] = "declare_queue";
        cmd["def"] = d.toJson();
        HttpResponse r = fromCommit(b.commitMetadata(cmd));
        if (r.status == 204) r.status = 201;
        return r;
    }
    broker::QueuePtr q;
    HttpResponse r = fromOp(vh->declareQueue(d, false, &q));
    if (r.status == 204) r.status = 201;
    return r;
}

HttpResponse deleteQueue(Broker& b, const HttpRequest& req, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requirePermission(b, vh->name(), Access::Configure, p.at("name"));
    auto q = vh->findQueue(p.at("name"));
    if (!q) notFound();
    bool ifEmpty = req.queryParam("if-empty") == "true";
    bool ifUnused = req.queryParam("if-unused") == "true";
    if (q->durable() && !q->exclusive()) {
        Json cmd = Json::object();
        cmd["op"] = "delete_queue";
        cmd["vhost"] = vh->name();
        cmd["name"] = q->name();
        cmd["if_empty"] = ifEmpty;
        cmd["if_unused"] = ifUnused;
        return fromCommit(b.commitMetadata(cmd));
    }
    return fromOp(vh->deleteQueue(q->name(), ifUnused, ifEmpty, nullptr));
}

HttpResponse purgeQueue(Broker& b, const HttpRequest&, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requirePermission(b, vh->name(), Access::Read, p.at("name"));
    auto q = vh->findQueue(p.at("name"));
    if (!q) notFound();
    q->purge();
    return HttpResponse::noContent();
}

HttpResponse getMessages(Broker& b, const HttpRequest& req, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requireVhostAccess(b, vh->name());
    auto q = vh->findQueue(p.at("name"));
    if (!q) notFound();
    Json bj = body(req);
    int64_t count = std::clamp<int64_t>(bj["count"].asInt(1), 1, 1000);
    std::string ackmode = bj["ackmode"].asString("ack_requeue_true");
    std::string encoding = bj["encoding"].asString("auto");
    int64_t truncate = bj["truncate"].asInt(-1);
    broker::Settlement how = broker::Settlement::Requeue;
    if (ackmode == "ack_requeue_false") how = broker::Settlement::Ack;
    else if (ackmode == "reject_requeue_false") how = broker::Settlement::Reject;
    Json arr = Json::array();
    std::vector<uint64_t> tags;
    for (int64_t i = 0; i < count; ++i) {
        auto d = q->basicGet(false);
        if (!d) break;
        tags.push_back(d->queueTag);
        Json m = Json::object();
        std::string payload = d->msg->body;
        if (truncate >= 0 && payload.size() > static_cast<size_t>(truncate)) payload.resize(static_cast<size_t>(truncate));
        if (encoding == "base64" || (encoding == "auto" && !str::isPrintableAscii(payload))) {
            m["payload"] = crypto::base64Encode(payload);
            m["payload_encoding"] = "base64";
        } else {
            m["payload"] = payload;
            m["payload_encoding"] = "string";
        }
        m["payload_bytes"] = static_cast<int64_t>(d->msg->body.size());
        m["redelivered"] = d->redelivered;
        m["exchange"] = d->msg->exchange;
        m["routing_key"] = d->msg->routingKey;
        m["message_count"] = static_cast<int64_t>(d->remaining);
        m["properties"] = d->msg->props.toJson();
        arr.push(m);
    }
    if (!tags.empty()) q->settle(tags, how);
    return HttpResponse::json(200, arr);
}

// ---------------------------------------------------------------- bindings

HttpResponse getBindings(Broker& b, const HttpRequest&, const PathParams& p) {
    Json arr = Json::array();
    for (const auto& vh : b.vhosts()) {
        if (p.count("vhost") && vh->name() != p.at("vhost")) continue;
        if (!canSeeVhost(b, vh->name())) continue;
        for (const auto& bd : vh->bindings()) arr.push(bd.toJson(vh->name()));
    }
    return HttpResponse::json(200, arr);
}

HttpResponse postBinding(Broker& b, const HttpRequest& req, const PathParams& p, broker::DestinationKind kind) {
    auto vh = vhostOr404(b, p.at("vhost"));
    const std::string& source = p.at("source");
    const std::string& destination = p.at("destination");
    requirePermission(b, vh->name(), Access::Write, destination);
    requirePermission(b, vh->name(), Access::Read, source);
    Json bj = body(req);
    broker::Binding bd;
    bd.source = source;
    bd.kind = kind;
    bd.destination = destination;
    bd.routingKey = bj["routing_key"].asString();
    bd.arguments = amqp::FieldTable::fromJson(bj["arguments"]);
    Json cmd = Json::object();
    cmd["op"] = "bind";
    cmd["vhost"] = vh->name();
    cmd["binding"] = bd.toJson(vh->name());
    HttpResponse r = fromCommit(b.commitMetadata(cmd));
    if (r.status == 204) {
        r.status = 201;
        r.headers["Location"] = str::urlEncode(bd.routingKey.empty() ? "~" : bd.routingKey);
    }
    return r;
}

HttpResponse deleteBinding(Broker& b, const HttpRequest&, const PathParams& p, broker::DestinationKind kind) {
    auto vh = vhostOr404(b, p.at("vhost"));
    const std::string& source = p.at("source");
    const std::string& destination = p.at("destination");
    requirePermission(b, vh->name(), Access::Write, destination);
    requirePermission(b, vh->name(), Access::Read, source);
    broker::Binding bd;
    bd.source = source;
    bd.kind = kind;
    bd.destination = destination;
    bd.routingKey = p.at("props") == "~" ? "" : p.at("props");
    Json cmd = Json::object();
    cmd["op"] = "unbind";
    cmd["vhost"] = vh->name();
    cmd["binding"] = bd.toJson(vh->name());
    return fromCommit(b.commitMetadata(cmd));
}

// ------------------------------------------------------------------- users

HttpResponse getUsers(Broker& b, const HttpRequest&, const PathParams&) {
    requireTag("monitoring");
    Json arr = Json::array();
    for (const auto& u : b.users().listUsers()) arr.push(u.toJson(true));
    return HttpResponse::json(200, arr);
}

HttpResponse getUser(Broker& b, const HttpRequest&, const PathParams& p) {
    requireAdmin();
    auto u = b.users().getUser(p.at("name"));
    if (!u) notFound();
    return HttpResponse::json(200, u->toJson(true));
}

HttpResponse putUser(Broker& b, const HttpRequest& req, const PathParams& p) {
    requireAdmin();
    Json bj = body(req);
    Json user = Json::object();
    user["name"] = p.at("name");
    if (bj.contains("password")) {
        user["password_hash"] = security::UserStore::hashPassword(bj["password"].asString());
    } else if (bj.contains("password_hash")) {
        user["password_hash"] = bj["password_hash"].asString();
        user["hashing_algorithm"] = bj["hashing_algorithm"].asString(security::kHashSha256);
    } else if (auto existing = b.users().getUser(p.at("name"))) {
        user["password_hash"] = existing->passwordHash;
    }
    user["tags"] = bj["tags"];
    Json cmd = Json::object();
    cmd["op"] = "put_user";
    cmd["user"] = user;
    HttpResponse r = fromCommit(b.commitMetadata(cmd));
    if (r.status == 204) r.status = 201;
    return r;
}

HttpResponse deleteUser(Broker& b, const HttpRequest&, const PathParams& p) {
    requireAdmin();
    Json cmd = Json::object();
    cmd["op"] = "delete_user";
    cmd["name"] = p.at("name");
    return fromCommit(b.commitMetadata(cmd));
}

HttpResponse whoami(Broker&, const HttpRequest&, const PathParams&) {
    Json j = Json::object();
    j["name"] = ctx().user.name;
    Json tags = Json::array();
    for (const auto& t : ctx().user.tags) tags.push(t);
    j["tags"] = tags;
    return HttpResponse::json(200, j);
}

// ------------------------------------------------------------- permissions

HttpResponse getPermissions(Broker& b, const HttpRequest&, const PathParams& p) {
    requireAdmin();
    Json arr = Json::array();
    std::string vhost = p.count("vhost") ? p.at("vhost") : "";
    std::string user = p.count("user") ? p.at("user") : "";
    for (const auto& perm : b.users().listPermissions(vhost, user)) arr.push(perm.toJson());
    if (!user.empty() && !vhost.empty()) {
        if (arr.size() == 0) notFound();
        return HttpResponse::json(200, arr.asArray()[0]);
    }
    return HttpResponse::json(200, arr);
}

HttpResponse putPermissions(Broker& b, const HttpRequest& req, const PathParams& p) {
    requireAdmin();
    Json bj = body(req);
    Json cmd = Json::object();
    cmd["op"] = "set_permissions";
    cmd["vhost"] = p.at("vhost");
    cmd["user"] = p.at("user");
    cmd["configure"] = bj["configure"].asString();
    cmd["write"] = bj["write"].asString();
    cmd["read"] = bj["read"].asString();
    HttpResponse r = fromCommit(b.commitMetadata(cmd));
    if (r.status == 204) r.status = 201;
    return r;
}

HttpResponse deletePermissions(Broker& b, const HttpRequest&, const PathParams& p) {
    requireAdmin();
    Json cmd = Json::object();
    cmd["op"] = "clear_permissions";
    cmd["vhost"] = p.at("vhost");
    cmd["user"] = p.at("user");
    return fromCommit(b.commitMetadata(cmd));
}

HttpResponse getTopicPermissions(Broker& b, const HttpRequest&, const PathParams& p) {
    requireAdmin();
    Json arr = Json::array();
    for (const auto& tp : b.users().listTopicPermissions(p.count("vhost") ? p.at("vhost") : "")) arr.push(tp.toJson());
    return HttpResponse::json(200, arr);
}

HttpResponse putTopicPermissions(Broker& b, const HttpRequest& req, const PathParams& p) {
    requireAdmin();
    Json bj = body(req);
    Json cmd = Json::object();
    cmd["op"] = "set_topic_permissions";
    cmd["vhost"] = p.at("vhost");
    cmd["user"] = p.at("user");
    cmd["exchange"] = bj["exchange"].asString();
    cmd["write"] = bj["write"].asString();
    cmd["read"] = bj["read"].asString();
    HttpResponse r = fromCommit(b.commitMetadata(cmd));
    if (r.status == 204) r.status = 201;
    return r;
}

// ---------------------------------------------------------------- policies

HttpResponse getPolicies(Broker& b, const HttpRequest&, const PathParams& p) {
    Json arr = Json::array();
    std::string vhost = p.count("vhost") ? p.at("vhost") : "";
    for (const auto& pol : b.policies().list(vhost)) {
        if (canSeeVhost(b, pol.vhost)) arr.push(pol.toJson());
    }
    return HttpResponse::json(200, arr);
}

HttpResponse getPolicy(Broker& b, const HttpRequest&, const PathParams& p) {
    requireVhostAccess(b, p.at("vhost"));
    auto pol = b.policies().get(p.at("vhost"), p.at("name"));
    if (!pol) notFound();
    return HttpResponse::json(200, pol->toJson());
}

HttpResponse putPolicy(Broker& b, const HttpRequest& req, const PathParams& p) {
    requireTag("policymaker");
    requireVhostAccess(b, p.at("vhost"));
    Json bj = body(req);
    bj["name"] = p.at("name");
    bj["vhost"] = p.at("vhost");
    Json cmd = Json::object();
    cmd["op"] = "set_policy";
    cmd["policy"] = bj;
    HttpResponse r = fromCommit(b.commitMetadata(cmd));
    if (r.status == 204) r.status = 201;
    return r;
}

HttpResponse deletePolicy(Broker& b, const HttpRequest&, const PathParams& p) {
    requireTag("policymaker");
    requireVhostAccess(b, p.at("vhost"));
    Json cmd = Json::object();
    cmd["op"] = "clear_policy";
    cmd["vhost"] = p.at("vhost");
    cmd["name"] = p.at("name");
    return fromCommit(b.commitMetadata(cmd));
}

// ------------------------------------------------------------- definitions

HttpResponse getDefinitions(Broker& b, const HttpRequest&, const PathParams&) {
    requireAdmin();
    return HttpResponse::json(200, b.exportDefinitions());
}

HttpResponse postDefinitions(Broker& b, const HttpRequest& req, const PathParams&) {
    requireTag("policymaker");
    Json defs = Json::parse(req.body);
    Json cmd = Json::object();
    cmd["op"] = "import_definitions";
    cmd["definitions"] = defs;
    return fromCommit(b.commitMetadata(cmd));
}

// ------------------------------------------------------------------ health

HttpResponse aliveness(Broker& b, const HttpRequest&, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requireVhostAccess(b, vh->name());
    broker::QueueDef d;
    d.name = "aliveness-test";
    d.vhost = vh->name();
    d.durable = false;
    broker::QueuePtr q;
    auto r = vh->declareQueue(d, false, &q);
    if (!r.ok() || !q) return HttpResponse::error(503, "failed", r.text);
    vh->publish("", broker::makeMessage("", "aliveness-test", {}, "test_message"));
    auto got = q->basicGet(true);
    Json j = Json::object();
    j["status"] = got ? "ok" : "failed";
    return HttpResponse::json(got ? 200 : 503, j);
}

HttpResponse healthAlarms(Broker& b, const HttpRequest&, const PathParams&) {
    Json j = Json::object();
    if (b.memoryAlarm()) {
        j["status"] = "failed";
        j["reason"] = "resource alarm(s) in effect: memory";
        return HttpResponse::json(503, j);
    }
    j["status"] = "ok";
    return HttpResponse::json(200, j);
}

HttpResponse healthVhosts(Broker& b, const HttpRequest&, const PathParams&) {
    Json j = Json::object();
    j["status"] = "ok";
    Json arr = Json::array();
    for (const auto& vh : b.vhosts()) arr.push(vh->name());
    j["vhosts"] = arr;
    return HttpResponse::json(200, j);
}

HttpResponse getLogFile(Broker& b, const HttpRequest&, const PathParams& p) {
    requireTag("monitoring");
    std::string logPath = Logger::instance().filePath();
    if (logPath.empty()) logPath = b.config().logFile;
    if (logPath.empty()) notFound("file logging is not enabled");
    std::string dir = std::filesystem::path(logPath).parent_path().string();
    auto content = fs::readFile(fs::join(dir, p.at("file")));
    if (!content) notFound();
    return HttpResponse::text(200, *content);
}

// ---------------------------------------------------------- super streams

HttpResponse putSuperStream(Broker& b, const HttpRequest& req, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    const std::string& name = p.at("name");
    requirePermission(b, vh->name(), Access::Configure, name);
    Json bj = body(req);
    int64_t partitions = bj["partitions"].asInt(3);
    if (partitions < 1 || partitions > 256) badRequest("partitions must be between 1 and 256");
    Json ex = Json::object();
    ex["op"] = "declare_exchange";
    broker::ExchangeDef ed;
    ed.name = name;
    ed.vhost = vh->name();
    ed.type = broker::ExchangeType::ConsistentHash;
    ex["def"] = ed.toJson();
    Json r = b.commitMetadata(ex);
    if (r["code"].asInt(0) != 0) return fromCommit(r);
    for (int64_t i = 0; i < partitions; ++i) {
        broker::QueueDef qd;
        qd.name = name + "-" + std::to_string(i);
        qd.vhost = vh->name();
        qd.type = broker::QueueType::Stream;
        qd.arguments.set("x-queue-type", amqp::FieldValue::longString("stream"));
        Json qc = Json::object();
        qc["op"] = "declare_queue";
        qc["def"] = qd.toJson();
        r = b.commitMetadata(qc);
        if (r["code"].asInt(0) != 0) return fromCommit(r);
        broker::Binding bd;
        bd.source = name;
        bd.destination = qd.name;
        bd.routingKey = "1";
        Json bc = Json::object();
        bc["op"] = "bind";
        bc["vhost"] = vh->name();
        bc["binding"] = bd.toJson(vh->name());
        b.commitMetadata(bc);
    }
    HttpResponse resp = HttpResponse::noContent();
    resp.status = 201;
    return resp;
}

HttpResponse metrics(Broker& b, const HttpRequest&, const PathParams&) {
    return HttpResponse::text(200, renderMetrics(b), "text/plain; version=0.0.4");
}

}  // namespace

std::string renderMetrics(Broker& b) {
    std::string out = MetricsRegistry::global().renderPrometheus();
    Json ov = b.overview();
    out += "# TYPE tropiqueue_connections gauge\ntropiqueue_connections " +
           std::to_string(ov["object_totals"]["connections"].asInt()) + "\n";
    out += "# TYPE tropiqueue_queues gauge\ntropiqueue_queues " + std::to_string(ov["object_totals"]["queues"].asInt()) +
           "\n";
    out += "# TYPE tropiqueue_memory_alarm gauge\ntropiqueue_memory_alarm " +
           std::string(b.memoryAlarm() ? "1" : "0") + "\n";
    out += "# TYPE tropiqueue_queue_messages_ready gauge\n";
    std::string unacked = "# TYPE tropiqueue_queue_messages_unacked gauge\n";
    std::string consumers = "# TYPE tropiqueue_queue_consumers gauge\n";
    for (const auto& vh : b.vhosts()) {
        for (const auto& q : vh->queues()) {
            std::string labels = "{vhost=\"" + vh->name() + "\",queue=\"" + q->name() + "\"}";
            out += "tropiqueue_queue_messages_ready" + labels + " " + std::to_string(q->messageCount()) + "\n";
            unacked += "tropiqueue_queue_messages_unacked" + labels + " " + std::to_string(q->unackedCount()) + "\n";
            consumers += "tropiqueue_queue_consumers" + labels + " " + std::to_string(q->consumerCount()) + "\n";
        }
    }
    return out + unacked + consumers;
}

void Routes::setContext(RequestContext* c) { g_ctx = c; }
RequestContext& Routes::context() { return util::ctx(); }

void Routes::registerAll(net::HttpRouter& r, Broker& b) {
    auto bind = [&b](HttpResponse (*fn)(Broker&, const HttpRequest&, const PathParams&)) {
        return [&b, fn](const HttpRequest& req, const PathParams& p) { return fn(b, req, p); };
    };
    r.add("GET", "/api/overview", bind(getOverview));
    r.add("GET", "/api/nodes", bind(getNodes));
    r.add("GET", "/api/cluster", bind(getCluster));
    r.add("GET", "/api/connections", bind(getConnections));
    r.add("GET", "/api/connections/{name}", bind(getConnection));
    r.add("DELETE", "/api/connections/{name}", bind(deleteConnection));
    r.add("GET", "/api/channels", bind(getChannels));
    r.add("GET", "/api/vhosts", bind(getVhosts));
    r.add("GET", "/api/vhosts/{vhost}", bind(getVhost));
    r.add("PUT", "/api/vhosts/{vhost}", bind(putVhost));
    r.add("DELETE", "/api/vhosts/{vhost}", bind(deleteVhost));
    r.add("GET", "/api/vhost-limits/{vhost}", bind(getVhostLimits));
    r.add("PUT", "/api/vhost-limits/{vhost}/{name}", bind(putVhostLimit));
    r.add("GET", "/api/exchanges", bind(getExchanges));
    r.add("GET", "/api/exchanges/{vhost}", bind(getExchanges));
    r.add("GET", "/api/exchanges/{vhost}/{name}", bind(getExchange));
    r.add("PUT", "/api/exchanges/{vhost}/{name}", bind(putExchange));
    r.add("DELETE", "/api/exchanges/{vhost}/{name}", bind(deleteExchange));
    r.add("POST", "/api/exchanges/{vhost}/{name}/publish", bind(publishToExchange));
    r.add("GET", "/api/queues", bind(getQueues));
    r.add("GET", "/api/queues/{vhost}", bind(getQueues));
    r.add("GET", "/api/queues/{vhost}/{name}", bind(getQueue));
    r.add("PUT", "/api/queues/{vhost}/{name}", bind(putQueue));
    r.add("DELETE", "/api/queues/{vhost}/{name}", bind(deleteQueue));
    r.add("DELETE", "/api/queues/{vhost}/{name}/contents", bind(purgeQueue));
    r.add("POST", "/api/queues/{vhost}/{name}/get", bind(getMessages));
    r.add("GET", "/api/bindings", bind(getBindings));
    r.add("GET", "/api/bindings/{vhost}", bind(getBindings));
    r.add("POST", "/api/bindings/{vhost}/e/{source}/q/{destination}",
          [&b](const HttpRequest& req, const PathParams& p) { return postBinding(b, req, p, broker::DestinationKind::Queue); });
    r.add("POST", "/api/bindings/{vhost}/e/{source}/e/{destination}",
          [&b](const HttpRequest& req, const PathParams& p) { return postBinding(b, req, p, broker::DestinationKind::Exchange); });
    r.add("DELETE", "/api/bindings/{vhost}/e/{source}/q/{destination}/{props}",
          [&b](const HttpRequest& req, const PathParams& p) { return deleteBinding(b, req, p, broker::DestinationKind::Queue); });
    r.add("DELETE", "/api/bindings/{vhost}/e/{source}/e/{destination}/{props}",
          [&b](const HttpRequest& req, const PathParams& p) { return deleteBinding(b, req, p, broker::DestinationKind::Exchange); });
    r.add("GET", "/api/users", bind(getUsers));
    r.add("GET", "/api/users/{name}", bind(getUser));
    r.add("PUT", "/api/users/{name}", bind(putUser));
    r.add("DELETE", "/api/users/{name}", bind(deleteUser));
    r.add("GET", "/api/whoami", bind(whoami));
    r.add("GET", "/api/permissions", bind(getPermissions));
    r.add("GET", "/api/permissions/{vhost}/{user}", bind(getPermissions));
    r.add("PUT", "/api/permissions/{vhost}/{user}", bind(putPermissions));
    r.add("DELETE", "/api/permissions/{vhost}/{user}", bind(deletePermissions));
    r.add("GET", "/api/topic-permissions", bind(getTopicPermissions));
    r.add("GET", "/api/topic-permissions/{vhost}", bind(getTopicPermissions));
    r.add("PUT", "/api/topic-permissions/{vhost}/{user}", bind(putTopicPermissions));
    r.add("GET", "/api/policies", bind(getPolicies));
    r.add("GET", "/api/policies/{vhost}", bind(getPolicies));
    r.add("GET", "/api/policies/{vhost}/{name}", bind(getPolicy));
    r.add("PUT", "/api/policies/{vhost}/{name}", bind(putPolicy));
    r.add("DELETE", "/api/policies/{vhost}/{name}", bind(deletePolicy));
    r.add("GET", "/api/definitions", bind(getDefinitions));
    r.add("POST", "/api/definitions", bind(postDefinitions));
    r.add("GET", "/api/aliveness-test/{vhost}", bind(aliveness));
    r.add("GET", "/api/health/checks/alarms", bind(healthAlarms));
    r.add("GET", "/api/health/checks/virtual-hosts", bind(healthVhosts));
    r.add("GET", "/api/logs/{file}", bind(getLogFile));
    r.add("PUT", "/api/super-streams/{vhost}/{name}", bind(putSuperStream));
    r.add("GET", "/metrics", bind(metrics));
    registerExtended(r, b);
}

}  // namespace tq::mgmt
