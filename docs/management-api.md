# Management HTTP API

The management API is served on port 15672 (`management.tcp.port`). It
follows the RabbitMQ management API: paths, verbs, request bodies and the
JSON shape of responses are the same, so existing scripts, dashboards and
tools such as `rabbitmqadmin` work against TropiQueue for the endpoints
listed here.

## Authentication

Requests authenticate with HTTP basic authentication or, when OAuth 2.0 is
enabled, with an access token:

```
Authorization: Basic Z3Vlc3Q6Z3Vlc3Q=
Authorization: Bearer eyJhbGciOiJIUzI1NiJ9....
```

The user must have one of the tags `management`, `policymaker`,
`monitoring` or `administrator`:

| Tag | Can |
|-----|-----|
| `management` | see and manage the vhosts they have permissions on, their own connections and channels |
| `policymaker` | everything `management` can, plus policies and runtime parameters (shovels, federation) in those vhosts |
| `monitoring` | read-only view of every vhost, connection and channel, node information |
| `administrator` | everything, including users, vhosts, permissions, global parameters and feature flags |

Errors are returned as JSON with an HTTP status code:

```json
{"error": "not_found", "reason": "vhost 'staging' not found"}
```

Names in paths are URL-encoded; the default vhost `/` is written `%2F`.

## Lists, filtering and paging

Collection endpoints (`/api/queues`, `/api/exchanges`, `/api/connections`,
`/api/parameters`, `/api/shovels`, ...) accept:

| Parameter | Meaning |
|-----------|---------|
| `name` | only items whose name contains this text (case-insensitive) |
| `use_regex=true` | interpret `name` as a regular expression |
| `sort` | sort by this field; nested fields use dots (`message_stats.publish`) |
| `sort_reverse=true` | reverse the order |
| `page`, `page_size` | return one page (page size 1-500, default 100) |

Without `page` the response is a plain JSON array. With `page` it is an
object:

```json
{
  "items": [ ... ],
  "page": 2, "page_count": 7, "page_size": 100,
  "filtered_count": 640, "item_count": 100, "total_count": 1210
}
```

## Overview and nodes

| Verb | Path | Description |
|------|------|-------------|
| GET | `/api/overview` | Version, node, cluster name, message and object totals. |
| GET | `/api/nodes` | Node list with memory use, memory limit and alarms (monitoring). |
| GET | `/api/cluster` | Cluster membership and Raft group leaders. |
| GET | `/api/whoami` | The authenticated user and its tags. |

## Connections and channels

| Verb | Path | Description |
|------|------|-------------|
| GET | `/api/connections` | AMQP connections (own connections, or all for monitoring users). |
| GET | `/api/connections/{name}` | One connection, including client properties. |
| DELETE | `/api/connections/{name}` | Close a connection. The optional `X-Reason` header is sent to the client. |
| GET | `/api/channels` | Channels per connection. |
| GET | `/api/stream/connections` | Stream protocol connections. |
| GET | `/api/stream/connections/{vhost}` | Stream protocol connections of one vhost. |

## Virtual hosts and limits

| Verb | Path | Description |
|------|------|-------------|
| GET | `/api/vhosts` | Vhosts visible to the user, with message counts and tracing state. |
| GET | `/api/vhosts/{vhost}` | One vhost. |
| PUT | `/api/vhosts/{vhost}` | Create a vhost (administrator). |
| DELETE | `/api/vhosts/{vhost}` | Delete a vhost and everything in it (administrator). |
| PUT | `/api/vhosts/{vhost}/tracing` | `{"tracing": true}` enables the firehose for the vhost. |
| GET | `/api/vhost-limits/{vhost}` | The vhost's limits. |
| PUT | `/api/vhost-limits/{vhost}/{name}` | `{"value": 100}` sets `max-connections` or `max-queues` (-1 removes). |

## Exchanges

| Verb | Path | Description |
|------|------|-------------|
| GET | `/api/exchanges`, `/api/exchanges/{vhost}` | List exchanges. |
| GET | `/api/exchanges/{vhost}/{name}` | One exchange. |
| PUT | `/api/exchanges/{vhost}/{name}` | Declare: `{"type": "topic", "durable": true, "auto_delete": false, "internal": false, "arguments": {}}`. |
| DELETE | `/api/exchanges/{vhost}/{name}` | Delete; `?if-unused=true` refuses when bindings exist. |
| POST | `/api/exchanges/{vhost}/{name}/publish` | Publish a message (see below). Use `amq.default` for the default exchange. |

Publishing:

```json
{
  "routing_key": "orders.created",
  "payload": "{\"id\": 42}",
  "payload_encoding": "string",
  "properties": {"delivery_mode": 2, "content_type": "application/json", "headers": {"source": "api"}}
}
```

The response is `{"routed": true}` or `{"routed": false}`.

## Queues

| Verb | Path | Description |
|------|------|-------------|
| GET | `/api/queues`, `/api/queues/{vhost}` | List queues with counters and `message_stats`. |
| GET | `/api/queues/{vhost}/{name}` | One queue, including consumer details. |
| PUT | `/api/queues/{vhost}/{name}` | Declare: `{"durable": true, "auto_delete": false, "arguments": {"x-queue-type": "quorum"}}`. |
| DELETE | `/api/queues/{vhost}/{name}` | Delete; `?if-unused=true` and `?if-empty=true` are supported. |
| DELETE | `/api/queues/{vhost}/{name}/contents` | Purge. |
| POST | `/api/queues/{vhost}/{name}/get` | Fetch messages (see below). |
| PUT | `/api/super-streams/{vhost}/{name}` | Create a super stream: `{"partitions": 3}`. |

Getting messages:

```json
{"count": 5, "ackmode": "ack_requeue_true", "encoding": "auto", "truncate": 50000}
```

`ackmode` is one of `ack_requeue_true` (look and put back),
`ack_requeue_false` (consume), `reject_requeue_true` and
`reject_requeue_false`. With `encoding: auto` bodies that are valid UTF-8
are returned as strings, other bodies base64-encoded
(`payload_encoding` tells which).

## Bindings

| Verb | Path | Description |
|------|------|-------------|
| GET | `/api/bindings`, `/api/bindings/{vhost}` | List bindings (including the implicit default-exchange bindings). |
| POST | `/api/bindings/{vhost}/e/{exchange}/q/{queue}` | Bind a queue: `{"routing_key": "a.*", "arguments": {}}`. |
| POST | `/api/bindings/{vhost}/e/{source}/e/{destination}` | Bind an exchange to an exchange. |
| DELETE | `/api/bindings/{vhost}/e/{exchange}/q/{queue}/{props}` | Unbind; `props` is the URL-encoded routing key (`~` for an empty key). |
| DELETE | `/api/bindings/{vhost}/e/{source}/e/{destination}/{props}` | Remove an exchange-to-exchange binding. |

## Users, permissions and limits

| Verb | Path | Description |
|------|------|-------------|
| GET | `/api/users` | Users (administrator). |
| GET | `/api/users/{name}` | One user. |
| PUT | `/api/users/{name}` | Create or update: `{"password": "...", "tags": "management,monitoring"}`, or `{"password_hash": "...", "hashing_algorithm": "tq_password_hashing_sha256", "tags": [...]}`. |
| DELETE | `/api/users/{name}` | Delete a user and their permissions. |
| GET | `/api/permissions` | All permissions. |
| GET | `/api/permissions/{vhost}/{user}` | One user's permissions in a vhost. |
| PUT | `/api/permissions/{vhost}/{user}` | `{"configure": ".*", "write": ".*", "read": ".*"}`. |
| DELETE | `/api/permissions/{vhost}/{user}` | Remove permissions. |
| GET | `/api/topic-permissions`, `/api/topic-permissions/{vhost}` | Topic permissions. |
| PUT | `/api/topic-permissions/{vhost}/{user}` | `{"exchange": "amq.topic", "write": "^orders\\.", "read": ".*"}`. |
| GET | `/api/user-limits`, `/api/user-limits/{user}` | User limits. |
| PUT | `/api/user-limits/{user}/{name}` | `{"value": 10}` for `max-connections` or `max-channels`. |
| DELETE | `/api/user-limits/{user}/{name}` | Remove a limit (`all` removes every limit). |

## Policies

| Verb | Path | Description |
|------|------|-------------|
| GET | `/api/policies`, `/api/policies/{vhost}` | List policies. |
| GET | `/api/policies/{vhost}/{name}` | One policy. |
| PUT | `/api/policies/{vhost}/{name}` | `{"pattern": "^orders\\.", "apply-to": "queues", "priority": 1, "definition": {"max-length": 100000}}`. |
| DELETE | `/api/policies/{vhost}/{name}` | Remove a policy. |

See [queues.md](queues.md#policies) for the definition keys.

## Runtime and global parameters

| Verb | Path | Description |
|------|------|-------------|
| GET | `/api/parameters` | All runtime parameters visible to the user (policymaker). |
| GET | `/api/parameters/{component}` | Parameters of one component. |
| GET | `/api/parameters/{component}/{vhost}` | Parameters of one component in a vhost. |
| GET | `/api/parameters/{component}/{vhost}/{name}` | One parameter. |
| PUT | `/api/parameters/{component}/{vhost}/{name}` | `{"value": {...}}`. |
| DELETE | `/api/parameters/{component}/{vhost}/{name}` | Remove a parameter. |
| GET | `/api/global-parameters` | Global parameters (administrator). |
| PUT | `/api/global-parameters/{name}` | `{"value": "eu-west"}`; `cluster_name` is used by federation and shown in the UI. |
| DELETE | `/api/global-parameters/{name}` | Remove a global parameter. |

Components: `shovel`, `federation-upstream`, `federation-upstream-set`.

## Shovels and federation

| Verb | Path | Description |
|------|------|-------------|
| GET | `/api/shovels`, `/api/shovels/{vhost}` | Shovel status: state, messages forwarded, last error. |
| DELETE | `/api/shovels/vhost/{vhost}/{name}/restart` | Restart a shovel. |
| GET | `/api/federation-links`, `/api/federation-links/{vhost}` | Federation link status (monitoring). |
| DELETE | `/api/federation-links/vhost/{vhost}/{exchange}/{upstream}/restart` | Restart a link. |

## Definitions

| Verb | Path | Description |
|------|------|-------------|
| GET | `/api/definitions` | Export users, vhosts, permissions, policies, parameters, exchanges, queues and bindings. |
| POST | `/api/definitions` | Import a definitions document (administrator). Existing objects are updated. |

## Health checks

Health checks return 200 with `{"status": "ok"}` when healthy and 503 with
a `reason` otherwise. They do not require authentication so that load
balancers and orchestrators can call them.

| Path | Healthy when |
|------|--------------|
| `/api/health/checks/alarms` | no resource alarm is in effect |
| `/api/health/checks/virtual-hosts` | every vhost is running |
| `/api/health/checks/port-listener/{port}` | a listener is bound to the port |
| `/api/health/checks/protocol-listener/{protocol}` | the protocol (`amqp`, `http`, `mqtt`, `stomp`, `stream`) has a listener |
| `/api/aliveness-test/{vhost}` | a test message can be published and consumed in the vhost (authenticated) |

## Feature flags

| Verb | Path | Description |
|------|------|-------------|
| GET | `/api/feature-flags` | Known flags with state and stability (monitoring). |
| PUT | `/api/feature-flags/{name}/enable` | Enable a flag and its dependencies (administrator). |

## Metrics

| Path | Description |
|------|-------------|
| `/metrics` | Prometheus text format: node totals and per-queue gauges. |
| `/metrics/detailed?family=queue_coarse_metrics,queue_consumer_count&vhost=/` | Per-object metrics (requires the `detailed_queue_metrics` feature flag and monitoring). |

## Logs

`GET /api/logs/{file}` returns a log file from the directory of `log.file`
(monitoring).

## Web UI

`GET /` serves the web UI from `management.www_dir`. Its files are under
`/ui/`. The UI uses the same API with the credentials entered on its login
page; they are kept in the browser's session storage until the tab is
closed or the user logs out.
