// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Helpers shared by the management API route handlers.
#pragma once

#include <string>

#include "broker/vhost.h"
#include "common/json.h"
#include "mgmt/routes.h"
#include "net/http.h"
#include "security/users.h"

namespace tq::broker {
class Broker;
}

namespace tq::mgmt::util {

// The authenticated user of the request being handled on this thread.
RequestContext& ctx();

[[noreturn]] void forbidden(const std::string& reason = "Access refused");
[[noreturn]] void notFound(const std::string& reason = "Object Not Found");
[[noreturn]] void badRequest(const std::string& reason);

void requireAdmin();
void requireTag(const std::string& tag);

// Parses the request body as a JSON object (empty body = {}).
Json body(const net::HttpRequest& req);

broker::VhostPtr vhostOr404(broker::Broker& b, const std::string& name);
bool canSeeVhost(broker::Broker& b, const std::string& vhost);
void requireVhostAccess(broker::Broker& b, const std::string& vhost);
void requirePermission(broker::Broker& b, const std::string& vhost, security::Access access,
                       const std::string& resource);

net::HttpResponse fromOp(const broker::OpResult& r);
net::HttpResponse fromCommit(const Json& r);

// Applies the list query parameters supported by the collection endpoints:
//
//   name=<text>          only items whose "name" contains text
//   use_regex=true       treat name as a regular expression
//   sort=<field>         sort by a top-level field (default: name)
//   sort_reverse=true
//   page=<n>, page_size=<n>
//
// Without "page" the (filtered, sorted) array is returned unchanged;
// with it the result is wrapped in the paging envelope used by RabbitMQ:
// {"items": [...], "page": n, "page_count": n, "page_size": n,
//  "filtered_count": n, "item_count": n, "total_count": n}.
Json applyListQuery(const net::HttpRequest& req, const Json& items);

}  // namespace tq::mgmt::util
