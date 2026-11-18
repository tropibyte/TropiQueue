# Migrating from RabbitMQ

TropiQueue implements the parts of RabbitMQ that applications and
operations tooling depend on, so most migrations need no code changes. This
guide lists what carries over, what to check, and two ways to move live
traffic.

## What carries over unchanged

- **Client libraries.** Any AMQP 0-9-1 client (Java, .NET, Python pika,
  Go amqp091, Node amqplib, ...) connects as it does to RabbitMQ, including
  publisher confirms, consumer cancel notifications, direct reply-to and
  connection blocking.
- **Stream clients.** Stream protocol clients connect to port 5552 once
  `stream.enabled = true` is set (see the message format note below).
- **MQTT 3.1.1 and STOMP 1.2 clients.**
- **Definitions.** `GET /api/definitions` from RabbitMQ can be imported
  with `tqctl import_definitions` or `load_definitions`. User password
  hashes created with `rabbit_password_hashing_sha256` are accepted, so
  users keep their passwords.
- **Management API scripts.** The endpoints listed in
  [management-api.md](management-api.md) have the same paths, verbs and
  JSON shapes.
- **Policies, arguments and header names** (`x-message-ttl`,
  `x-dead-letter-exchange`, `x-death`, `x-delivery-count`,
  `x-stream-offset`, ...).
- **Prometheus scraping** on `/metrics` (metric names differ: they are
  prefixed with `tropiqueue_`).

## What to check

| Area | RabbitMQ | TropiQueue |
|------|----------|------------|
| Configuration | `rabbitmq.conf` | `tropiqueue.conf` uses the same format and most key names; run `tqd --check-config` to find keys that do not exist. Advanced (`advanced.config`) settings have no equivalent. |
| TLS | built in | terminate TLS in a proxy |
| Plugins | Erlang plugins | built-in features only: management, Prometheus, shovel, federation (exchanges), MQTT, STOMP, streams, event exchange, tracing, consistent hash and delayed message exchanges, OAuth 2.0 |
| Classic mirrored queues | removed in RabbitMQ 4 | not supported; use quorum queues |
| Queue federation | supported | not supported; use a shovel |
| AMQP 1.0 | supported in 4.x | not supported |
| Stream message format | AMQP 1.0 encoded | opaque bytes; stream clients that set AMQP 1.0 properties should put them in the body |
| OAuth 2.0 | RS256 with JWKS, many options | HS256 with configured keys |
| `rabbitmqctl` / `rabbitmq-diagnostics` | CLI over Erlang distribution | `tqctl` over the management API |
| Erlang cookie | `.erlang.cookie` | `cluster.cookie` |

Key names that differ:

| RabbitMQ | TropiQueue |
|----------|------------|
| `management.tcp.port` | same |
| `cluster_formation.classic_config.nodes.1 = rabbit@a` | `cluster.nodes = 1@a:25672, ...` |
| `collect_statistics_interval` | not needed; statistics are live |
| `disk_free_limit.*` | not implemented; monitor disk space externally |

## Moving traffic

### Option 1: switch over

For systems that can pause briefly:

1. Export definitions from RabbitMQ and import them into TropiQueue.
2. Stop publishers.
3. Let consumers drain the RabbitMQ queues.
4. Point publishers and consumers at TropiQueue and start them.

### Option 2: shovel the backlog

For systems that must keep running:

1. Import the definitions into TropiQueue.
2. Point consumers at TropiQueue first; they will idle.
3. Point publishers at TropiQueue.
4. For every queue that still has messages in RabbitMQ, create a shovel on
   TropiQueue that drains it:

   ```sh
   tqctl set_parameter shovel drain-orders '{
     "src-uri": "amqp://migrate:secret@rabbit-01/%2F",
     "src-queue": "orders",
     "src-delete-after": "queue-length",
     "dest-uri": "amqp://migrate:secret@localhost/%2F",
     "dest-queue": "orders"
   }'
   ```

   With `src-delete-after: queue-length` each shovel removes itself after
   moving the messages that were in the queue when it started.
5. When `tqctl shovel_status` shows no shovels left, decommission
   RabbitMQ.

Consumers must be idempotent during the migration: with the default
`on-confirm` mode a message can be delivered twice if a shovel restarts
mid-batch.

## Verifying

- Compare `tqctl list_queues` with `rabbitmqctl list_queues` after the
  import.
- Run `tq-perf` against a staging node with your message sizes and queue
  types to size the hardware.
- Watch `tq-top` or the Prometheus metrics during the first days.
