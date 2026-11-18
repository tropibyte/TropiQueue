// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Management API: runtime parameters, global parameters, dynamic shovels,
// federation links, user limits, list paging and the web UI files.
#include <algorithm>
#include <regex>
#include <set>

#include "broker/broker.h"
#include "broker/parameters.h"
#include "common/fileio.h"
#include "common/strings.h"
#include "federation/federation.h"
#include "mgmt/route_util.h"
#include "mgmt/routes.h"
#include "shovel/shovel.h"
#include "streamproto/stream_server.h"

namespace tq::mgmt {

using broker::Broker;
using net::HttpRequest;
using net::HttpResponse;
using net::PathParams;

namespace util {

namespace {

bool fieldLess(const Json& a, const Json& b, const std::string& field) {
    const Json& x = a[field];
    const Json& y = b[field];
    if (x.isNumber() && y.isNumber()) return x.asDouble() < y.asDouble();
    if (x.isNumber() != y.isNumber()) return x.isNumber();
    return x.asString() < y.asString();
}

// Supports nested fields such as "message_stats.publish".
Json fieldValue(const Json& item, const std::string& path) {
    const Json* cur = &item;
    for (const auto& part : str::split(path, '.')) {
        const Json* next = cur->find(part);
        if (!next) return Json();
        cur = next;
    }
    return *cur;
}

}  // namespace

Json applyListQuery(const HttpRequest& req, const Json& items) {
    std::string name = req.queryParam("name");
    bool useRegex = req.queryParam("use_regex") == "true";
    std::string sortField = req.queryParam("sort");
    bool reverse = req.queryParam("sort_reverse") == "true";
    std::string pageParam = req.queryParam("page");

    std::vector<Json> filtered;
    filtered.reserve(items.size());
    if (name.empty()) {
        for (const auto& it : items.asArray()) filtered.push_back(it);
    } else if (useRegex) {
        std::regex re;
        try {
            re = std::regex(name, std::regex::ECMAScript | std::regex::icase);
        } catch (const std::regex_error& e) {
            badRequest(std::string("invalid regular expression: ") + e.what());
        }
        for (const auto& it : items.asArray()) {
            if (std::regex_search(it["name"].asString(), re)) filtered.push_back(it);
        }
    } else {
        std::string needle = str::toLower(name);
        for (const auto& it : items.asArray()) {
            if (str::toLower(it["name"].asString()).find(needle) != std::string::npos) filtered.push_back(it);
        }
    }

    if (!sortField.empty() || !pageParam.empty()) {
        std::string field = sortField.empty() ? "name" : sortField;
        std::stable_sort(filtered.begin(), filtered.end(), [&](const Json& a, const Json& b) {
            Json fa = Json::object(), fb = Json::object();
            fa["v"] = fieldValue(a, field);
            fb["v"] = fieldValue(b, field);
            return fieldLess(fa, fb, "v");
        });
        if (reverse) std::reverse(filtered.begin(), filtered.end());
    }

    if (pageParam.empty()) {
        Json out = Json::array();
        for (auto& it : filtered) out.push(std::move(it));
        return out;
    }

    auto page = str::parseInt(pageParam);
    auto pageSize = str::parseInt(req.queryParam("page_size").empty() ? "100" : req.queryParam("page_size"));
    if (!page || *page < 1) badRequest("page must be a positive integer");
    if (!pageSize || *pageSize < 1 || *pageSize > 500) badRequest("page_size must be between 1 and 500");
    int64_t filteredCount = static_cast<int64_t>(filtered.size());
    int64_t pageCount = filteredCount == 0 ? 1 : (filteredCount + *pageSize - 1) / *pageSize;
    if (*page > pageCount) badRequest("page " + std::to_string(*page) + " is beyond the last page");
    int64_t from = (*page - 1) * *pageSize;
    int64_t to = std::min(filteredCount, from + *pageSize);
    Json pageItems = Json::array();
    for (int64_t i = from; i < to; ++i) pageItems.push(std::move(filtered[static_cast<size_t>(i)]));

    Json out = Json::object();
    out["filtered_count"] = filteredCount;
    out["item_count"] = to - from;
    out["items"] = pageItems;
    out["page"] = *page;
    out["page_count"] = pageCount;
    out["page_size"] = *pageSize;
    out["total_count"] = static_cast<int64_t>(items.size());
    return out;
}

}  // namespace util

namespace {

using namespace util;

// ----------------------------------------------------------- runtime parameters

void requireParameterAccess(Broker& b, const std::string& vhost) {
    if (!ctx().hasTag("policymaker")) forbidden("Not policymaker user");
    if (!ctx().isAdmin() && !b.users().canAccessVhost(ctx().user.name, vhost)) {
        forbidden("Access refused to vhost '" + vhost + "'");
    }
}

HttpResponse getParameters(Broker& b, const HttpRequest& req, const PathParams& p) {
    if (!ctx().hasTag("policymaker")) forbidden("Not policymaker user");
    std::string component = p.count("component") ? p.at("component") : "";
    std::string vhost = p.count("vhost") ? p.at("vhost") : "";
    if (!component.empty() && !b.parameters().knownComponent(component)) notFound("unknown component");
    if (!vhost.empty()) vhostOr404(b, vhost);
    Json arr = Json::array();
    for (const auto& param : b.parameters().list(component, vhost)) {
        if (!ctx().isAdmin() && !b.users().canAccessVhost(ctx().user.name, param.vhost)) continue;
        arr.push(param.toJson());
    }
    return HttpResponse::json(200, applyListQuery(req, arr));
}

HttpResponse getParameter(Broker& b, const HttpRequest&, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requireParameterAccess(b, vh->name());
    auto param = b.parameters().get(p.at("component"), vh->name(), p.at("name"));
    if (!param) notFound();
    return HttpResponse::json(200, param->toJson());
}

HttpResponse putParameter(Broker& b, const HttpRequest& req, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requireParameterAccess(b, vh->name());
    if (!b.parameters().knownComponent(p.at("component"))) badRequest("unknown component '" + p.at("component") + "'");
    Json bj = body(req);
    if (!bj.contains("value")) badRequest("parameter body must contain a \"value\"");
    Json param = Json::object();
    param["component"] = p.at("component");
    param["vhost"] = vh->name();
    param["name"] = p.at("name");
    param["value"] = bj["value"];
    Json cmd = Json::object();
    cmd["op"] = "set_parameter";
    cmd["parameter"] = param;
    HttpResponse r = fromCommit(b.commitMetadata(cmd));
    if (r.status == 204) r.status = 201;
    return r;
}

HttpResponse deleteParameter(Broker& b, const HttpRequest&, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requireParameterAccess(b, vh->name());
    Json cmd = Json::object();
    cmd["op"] = "clear_parameter";
    cmd["component"] = p.at("component");
    cmd["vhost"] = vh->name();
    cmd["name"] = p.at("name");
    return fromCommit(b.commitMetadata(cmd));
}

// ------------------------------------------------------------ global parameters

HttpResponse getGlobalParameters(Broker& b, const HttpRequest&, const PathParams&) {
    requireAdmin();
    Json arr = Json::array();
    for (const auto& [name, value] : b.parameters().globals()) {
        Json g = Json::object();
        g["name"] = name;
        g["value"] = value;
        arr.push(g);
    }
    return HttpResponse::json(200, arr);
}

HttpResponse getGlobalParameter(Broker& b, const HttpRequest&, const PathParams& p) {
    requireAdmin();
    auto v = b.parameters().global(p.at("name"));
    if (!v) notFound();
    Json g = Json::object();
    g["name"] = p.at("name");
    g["value"] = *v;
    return HttpResponse::json(200, g);
}

HttpResponse putGlobalParameter(Broker& b, const HttpRequest& req, const PathParams& p) {
    requireAdmin();
    Json bj = body(req);
    if (!bj.contains("value")) badRequest("global parameter body must contain a \"value\"");
    Json cmd = Json::object();
    cmd["op"] = "set_global_parameter";
    cmd["name"] = p.at("name");
    cmd["value"] = bj["value"];
    HttpResponse r = fromCommit(b.commitMetadata(cmd));
    if (r.status == 204) r.status = 201;
    return r;
}

HttpResponse deleteGlobalParameter(Broker& b, const HttpRequest&, const PathParams& p) {
    requireAdmin();
    Json cmd = Json::object();
    cmd["op"] = "clear_global_parameter";
    cmd["name"] = p.at("name");
    return fromCommit(b.commitMetadata(cmd));
}

// --------------------------------------------------------------------- shovels

HttpResponse getShovels(Broker& b, const HttpRequest& req, const PathParams& p) {
    Json arr = Json::array();
    if (!b.shovels()) return HttpResponse::json(200, arr);
    std::string vhost = p.count("vhost") ? p.at("vhost") : "";
    if (!vhost.empty()) {
        vhostOr404(b, vhost);
        requireVhostAccess(b, vhost);
    }
    for (auto& s : b.shovels()->status(vhost)) {
        if (!canSeeVhost(b, s["vhost"].asString())) continue;
        arr.push(std::move(s));
    }
    return HttpResponse::json(200, applyListQuery(req, arr));
}

HttpResponse restartShovel(Broker& b, const HttpRequest&, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requireParameterAccess(b, vh->name());
    if (!b.shovels() || !b.shovels()->restart(vh->name(), p.at("name"))) notFound("shovel not found");
    return HttpResponse::noContent();
}

// ------------------------------------------------------------- federation links

HttpResponse getFederationLinks(Broker& b, const HttpRequest& req, const PathParams& p) {
    requireTag("monitoring");
    Json arr = Json::array();
    if (!b.federation()) return HttpResponse::json(200, arr);
    std::string vhost = p.count("vhost") ? p.at("vhost") : "";
    if (!vhost.empty()) vhostOr404(b, vhost);
    for (auto& l : b.federation()->status(vhost)) arr.push(std::move(l));
    return HttpResponse::json(200, applyListQuery(req, arr));
}

HttpResponse restartFederationLink(Broker& b, const HttpRequest&, const PathParams& p) {
    auto vh = vhostOr404(b, p.at("vhost"));
    requireParameterAccess(b, vh->name());
    if (!b.federation() || !b.federation()->restartLink(vh->name(), p.at("exchange"), p.at("upstream"))) {
        notFound("federation link not found");
    }
    return HttpResponse::noContent();
}

// --------------------------------------------------------------- feature flags

HttpResponse getFeatureFlags(Broker& b, const HttpRequest&, const PathParams&) {
    requireTag("monitoring");
    return HttpResponse::json(200, b.featureFlags().toJson());
}

HttpResponse enableFeatureFlag(Broker& b, const HttpRequest&, const PathParams& p) {
    requireAdmin();
    if (!broker::FeatureFlags::find(p.at("name"))) notFound("unknown feature flag");
    Json cmd = Json::object();
    cmd["op"] = "enable_feature_flag";
    cmd["name"] = p.at("name");
    return fromCommit(b.commitMetadata(cmd));
}

// ------------------------------------------------------------ detailed metrics

std::string labelValue(const std::string& v) {
    std::string out;
    out.reserve(v.size());
    for (char c : v) {
        if (c == '\\') out += "\\\\";
        else if (c == '"') out += "\\\"";
        else if (c == '\n') out += "\\n";
        else out += c;
    }
    return out;
}

HttpResponse detailedMetrics(Broker& b, const HttpRequest& req, const PathParams&) {
    requireTag("monitoring");
    if (!b.featureFlags().isEnabled("detailed_queue_metrics")) {
        throw RouteError(400, "bad_request", "feature flag detailed_queue_metrics is not enabled");
    }
    std::set<std::string> families;
    for (const auto& f : str::splitTrimmed(req.queryParam("family"), ',')) families.insert(f);
    if (families.empty()) families = {"queue_coarse_metrics", "queue_consumer_count", "vhost_status"};
    std::string vhostFilter = req.queryParam("vhost");
    std::string out;
    auto vhosts = b.vhosts();
    if (families.count("queue_coarse_metrics")) {
        std::string ready = "# TYPE tropiqueue_detailed_queue_messages_ready gauge\n";
        std::string unacked = "# TYPE tropiqueue_detailed_queue_messages_unacked gauge\n";
        std::string bytes = "# TYPE tropiqueue_detailed_queue_messages_bytes gauge\n";
        for (const auto& vh : vhosts) {
            if (!vhostFilter.empty() && vh->name() != vhostFilter) continue;
            for (const auto& q : vh->queues()) {
                std::string labels =
                    "{vhost=\"" + labelValue(vh->name()) + "\",queue=\"" + labelValue(q->name()) + "\"}";
                ready += "tropiqueue_detailed_queue_messages_ready" + labels + " " + std::to_string(q->messageCount()) + "\n";
                unacked += "tropiqueue_detailed_queue_messages_unacked" + labels + " " +
                           std::to_string(q->unackedCount()) + "\n";
                bytes += "tropiqueue_detailed_queue_messages_bytes" + labels + " " + std::to_string(q->messageBytes()) +
                         "\n";
            }
        }
        out += ready + unacked + bytes;
    }
    if (families.count("queue_consumer_count")) {
        out += "# TYPE tropiqueue_detailed_queue_consumers gauge\n";
        for (const auto& vh : vhosts) {
            if (!vhostFilter.empty() && vh->name() != vhostFilter) continue;
            for (const auto& q : vh->queues()) {
                out += "tropiqueue_detailed_queue_consumers{vhost=\"" + labelValue(vh->name()) + "\",queue=\"" +
                       labelValue(q->name()) + "\"} " + std::to_string(q->consumerCount()) + "\n";
            }
        }
    }
    if (families.count("vhost_status")) {
        out += "# TYPE tropiqueue_detailed_vhost_status gauge\n";
        for (const auto& vh : vhosts) {
            if (!vhostFilter.empty() && vh->name() != vhostFilter) continue;
            out += "tropiqueue_detailed_vhost_status{vhost=\"" + labelValue(vh->name()) + "\"} 1\n";
        }
    }
    return HttpResponse::text(200, out, "text/plain; version=0.0.4");
}

// ------------------------------------------------------------- health checks

HttpResponse healthPortListener(Broker& b, const HttpRequest&, const PathParams& p) {
    auto port = str::parseInt(p.at("port"));
    if (!port) badRequest("port must be a number");
    std::vector<uint16_t> ports = {b.amqpPort(), b.managementPort(), b.mqttPort(), b.stompPort(), b.streamPort()};
    bool listening = std::find(ports.begin(), ports.end(), static_cast<uint16_t>(*port)) != ports.end() && *port != 0;
    Json j = Json::object();
    j["status"] = listening ? "ok" : "failed";
    j["port"] = *port;
    if (!listening) j["reason"] = "no listener on port " + p.at("port");
    return HttpResponse::json(listening ? 200 : 503, j);
}

HttpResponse healthProtocolListener(Broker& b, const HttpRequest&, const PathParams& p) {
    std::string proto = str::toLower(p.at("protocol"));
    uint16_t port = 0;
    if (proto == "amqp" || proto == "amqp091" || proto == "amqp0-9-1") port = b.amqpPort();
    else if (proto == "http" || proto == "management") port = b.managementPort();
    else if (proto == "mqtt") port = b.mqttPort();
    else if (proto == "stomp") port = b.stompPort();
    else if (proto == "stream") port = b.streamPort();
    else badRequest("unknown protocol '" + p.at("protocol") + "'");
    Json j = Json::object();
    j["status"] = port ? "ok" : "failed";
    j["protocol"] = proto;
    if (port) j["port"] = static_cast<int64_t>(port);
    else j["reason"] = "no listener for protocol " + proto;
    return HttpResponse::json(port ? 200 : 503, j);
}

// ----------------------------------------------------------- stream protocol

HttpResponse getStreamConnections(Broker& b, const HttpRequest& req, const PathParams& p) {
    Json arr = Json::array();
    if (!b.streamServer()) return HttpResponse::json(200, arr);
    std::string vhost = p.count("vhost") ? p.at("vhost") : "";
    bool all = ctx().hasTag("monitoring");
    for (auto& c : b.streamServer()->connections()) {
        if (!vhost.empty() && c["vhost"].asString() != vhost) continue;
        if (!all && c["user"].asString() != ctx().user.name) continue;
        arr.push(std::move(c));
    }
    return HttpResponse::json(200, applyListQuery(req, arr));
}

// ------------------------------------------------------------------ user limits

Json userLimitsJson(const security::User& u) {
    Json j = Json::object();
    j["user"] = u.name;
    j["value"] = u.limits.toJson();
    return j;
}

HttpResponse getUserLimits(Broker& b, const HttpRequest&, const PathParams& p) {
    Json arr = Json::array();
    if (p.count("user")) {
        if (!ctx().isAdmin() && ctx().user.name != p.at("user")) forbidden();
        auto u = b.users().getUser(p.at("user"));
        if (!u) notFound();
        if (!u->limits.empty()) arr.push(userLimitsJson(*u));
        return HttpResponse::json(200, arr);
    }
    requireAdmin();
    for (const auto& u : b.users().listUsers()) {
        if (!u.limits.empty()) arr.push(userLimitsJson(u));
    }
    return HttpResponse::json(200, arr);
}

HttpResponse putUserLimit(Broker& b, const HttpRequest& req, const PathParams& p) {
    requireAdmin();
    const std::string& name = p.at("name");
    if (name != "max-connections" && name != "max-channels") badRequest("unknown user limit '" + name + "'");
    Json bj = body(req);
    if (!bj["value"].isNumber()) badRequest("limit value must be a number");
    Json limits = Json::object();
    limits[name] = bj["value"].asInt(-1);
    Json cmd = Json::object();
    cmd["op"] = "set_user_limits";
    cmd["user"] = p.at("user");
    cmd["limits"] = limits;
    return fromCommit(b.commitMetadata(cmd));
}

HttpResponse deleteUserLimit(Broker& b, const HttpRequest&, const PathParams& p) {
    requireAdmin();
    Json cmd = Json::object();
    cmd["op"] = "clear_user_limits";
    cmd["user"] = p.at("user");
    cmd["name"] = p.at("name");
    return fromCommit(b.commitMetadata(cmd));
}

// --------------------------------------------------------------------- tracing

HttpResponse putTracing(Broker& b, const HttpRequest& req, const PathParams& p) {
    requireAdmin();
    auto vh = vhostOr404(b, p.at("vhost"));
    Json bj = body(req);
    Json cmd = Json::object();
    cmd["op"] = "set_vhost_tracing";
    cmd["vhost"] = vh->name();
    cmd["tracing"] = bj["tracing"].asBool(false);
    return fromCommit(b.commitMetadata(cmd));
}

// ---------------------------------------------------------------------- web UI

const char* contentTypeFor(const std::string& file) {
    if (str::endsWith(file, ".html")) return "text/html; charset=utf-8";
    if (str::endsWith(file, ".js")) return "application/javascript; charset=utf-8";
    if (str::endsWith(file, ".css")) return "text/css; charset=utf-8";
    if (str::endsWith(file, ".svg")) return "image/svg+xml";
    return "application/octet-stream";
}

HttpResponse serveStatic(Broker& b, const std::string& file) {
    static const char* const kFiles[] = {"index.html", "app.js", "api.js", "views.js", "style.css", "logo.svg"};
    bool known = false;
    for (const char* f : kFiles) known = known || file == f;
    if (!known) notFound();
    auto content = fs::readFile(fs::join(b.config().managementWwwDir, file));
    if (!content) notFound("web UI file '" + file + "' is not installed");
    HttpResponse r = HttpResponse::text(200, *content, contentTypeFor(file));
    r.headers["Cache-Control"] = "no-cache";
    r.headers["X-Content-Type-Options"] = "nosniff";
    return r;
}

}  // namespace

void Routes::registerExtended(net::HttpRouter& r, Broker& b) {
    auto bind = [&b](HttpResponse (*fn)(Broker&, const HttpRequest&, const PathParams&)) {
        return [&b, fn](const HttpRequest& req, const PathParams& p) { return fn(b, req, p); };
    };
    r.add("GET", "/api/parameters", bind(getParameters));
    r.add("GET", "/api/parameters/{component}", bind(getParameters));
    r.add("GET", "/api/parameters/{component}/{vhost}", bind(getParameters));
    r.add("GET", "/api/parameters/{component}/{vhost}/{name}", bind(getParameter));
    r.add("PUT", "/api/parameters/{component}/{vhost}/{name}", bind(putParameter));
    r.add("DELETE", "/api/parameters/{component}/{vhost}/{name}", bind(deleteParameter));
    r.add("GET", "/api/global-parameters", bind(getGlobalParameters));
    r.add("GET", "/api/global-parameters/{name}", bind(getGlobalParameter));
    r.add("PUT", "/api/global-parameters/{name}", bind(putGlobalParameter));
    r.add("DELETE", "/api/global-parameters/{name}", bind(deleteGlobalParameter));
    r.add("GET", "/api/shovels", bind(getShovels));
    r.add("GET", "/api/shovels/{vhost}", bind(getShovels));
    r.add("DELETE", "/api/shovels/vhost/{vhost}/{name}/restart", bind(restartShovel));
    r.add("GET", "/api/federation-links", bind(getFederationLinks));
    r.add("GET", "/api/federation-links/{vhost}", bind(getFederationLinks));
    r.add("DELETE", "/api/federation-links/vhost/{vhost}/{exchange}/{upstream}/restart", bind(restartFederationLink));
    r.add("GET", "/api/feature-flags", bind(getFeatureFlags));
    r.add("PUT", "/api/feature-flags/{name}/enable", bind(enableFeatureFlag));
    r.add("GET", "/metrics/detailed", bind(detailedMetrics));
    r.add("GET", "/api/health/checks/port-listener/{port}", bind(healthPortListener));
    r.add("GET", "/api/health/checks/protocol-listener/{protocol}", bind(healthProtocolListener));
    r.add("GET", "/api/stream/connections", bind(getStreamConnections));
    r.add("GET", "/api/stream/connections/{vhost}", bind(getStreamConnections));
    r.add("GET", "/api/user-limits", bind(getUserLimits));
    r.add("GET", "/api/user-limits/{user}", bind(getUserLimits));
    r.add("PUT", "/api/user-limits/{user}/{name}", bind(putUserLimit));
    r.add("DELETE", "/api/user-limits/{user}/{name}", bind(deleteUserLimit));
    r.add("PUT", "/api/vhosts/{vhost}/tracing", bind(putTracing));

    r.add("GET", "/", [&b](const HttpRequest&, const PathParams&) { return serveStatic(b, "index.html"); });
    r.add("GET", "/ui/{file}", [&b](const HttpRequest&, const PathParams& p) { return serveStatic(b, p.at("file")); });
}

}  // namespace tq::mgmt
