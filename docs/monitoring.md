# Monitoring

## Prometheus

`GET /metrics` on the management port returns metrics in the Prometheus
text format. It needs no authentication so that Prometheus can scrape it
without credentials; restrict access to the port if the metrics are
sensitive.

```yaml
scrape_configs:
  - job_name: tropiqueue
    static_configs:
      - targets: ["mq-01:15672", "mq-02:15672", "mq-03:15672"]
```

| Metric | Type | Meaning |
|--------|------|---------|
| `tropiqueue_connections` | gauge | open AMQP connections |
| `tropiqueue_queues` | gauge | queues on the node |
| `tropiqueue_memory_alarm` | gauge | 1 while the memory alarm is in effect |
| `tropiqueue_queue_messages_ready{vhost,queue}` | gauge | ready messages per queue |
| `tropiqueue_queue_messages_unacked{vhost,queue}` | gauge | unacknowledged messages per queue |
| `tropiqueue_queue_consumers{vhost,queue}` | gauge | consumers per queue |
| `tropiqueue_messages_published_total` | counter | messages published by AMQP clients |
| `tropiqueue_messages_delivered_total` | counter | deliveries to consumers and `basic.get` |
| `tropiqueue_messages_acknowledged_total` | counter | deliveries acknowledged |
| `tropiqueue_messages_redelivered_total` | counter | deliveries with the redelivered flag |
| `tropiqueue_messages_confirmed_total` | counter | publisher confirms sent |
| `tropiqueue_messages_unroutable_total` | counter | messages that matched no queue |
| `tropiqueue_messages_returned_total` | counter | mandatory messages returned |
| `tropiqueue_connections_opened_total`, `..._closed_total` | counter | AMQP connection churn |
| `tropiqueue_channels_opened_total` | counter | AMQP channel churn |
| `tropiqueue_auth_attempts_failed_total` | counter | failed logins |
| `tropiqueue_store_sync_seconds` | histogram | duration of message store syncs |

On brokers with many queues the per-queue series can be large. With the
experimental `detailed_queue_metrics` feature flag enabled,
`/metrics/detailed` (monitoring users) serves selected metric families for
selected vhosts:

```
GET /metrics/detailed?family=queue_coarse_metrics,queue_consumer_count&vhost=orders
```

Families: `queue_coarse_metrics` (ready, unacked, bytes),
`queue_consumer_count` and `vhost_status`.

## Health checks

Health checks are unauthenticated and return 200 or 503:

| Endpoint | Use as |
|----------|--------|
| `/api/health/checks/alarms` | readiness: fails while publishers are blocked |
| `/api/health/checks/virtual-hosts` | readiness |
| `/api/health/checks/protocol-listener/amqp` | liveness of the AMQP listener |
| `/api/health/checks/port-listener/5672` | liveness of a specific port |

Example Kubernetes probes:

```yaml
livenessProbe:
  httpGet: {path: /api/health/checks/protocol-listener/amqp, port: 15672}
readinessProbe:
  httpGet: {path: /api/health/checks/alarms, port: 15672}
```

## Resource alarms

The broker tracks the memory used by message bodies held in memory. When
it exceeds `vm_memory_high_watermark.relative` of the machine's RAM (or
`vm_memory_high_watermark.absolute`), the memory alarm is raised:
publishing connections are blocked (`connection.blocked` is sent to
clients that support it) until consumers have drained enough messages.
Consumers are never blocked. The alarm is shown in `/api/nodes`, in the web
UI and by `tq-top`, and fails the alarms health check.

## The event exchange

The broker publishes internal events to the topic exchange
`amq.rabbitmq.event` of the vhost they concern. Bind a queue to watch them:

```sh
tqctl declare_queue broker-events
tqctl bind_queue amq.rabbitmq.event broker-events 'queue.#'
```

Events have the event name as routing key and their details as message
headers (all values are strings, plus `timestamp_in_ms` and `node`).

| Routing key | Details |
|-------------|---------|
| `connection.created`, `connection.closed` | `name`, `user`, `peer_host`, `protocol` |
| `consumer.created`, `consumer.deleted` | `consumer_tag`, `queue`, `channel`, `connection` |
| `queue.created`, `queue.deleted` | `name`, `type`, `durable`, `exclusive` |
| `exchange.created`, `exchange.deleted` | `name`, `type`, `durable` |
| `binding.created`, `binding.deleted` | `source_name`, `destination_name`, `destination_kind`, `routing_key` |
| `policy.set`, `policy.cleared` | `name`, `pattern` |
| `parameter.set`, `parameter.cleared` | `component`, `name` |
| `permission.created`, `permission.deleted` | `user`, `configure`, `write`, `read` |
| `vhost.created`, `vhost.deleted` | `name` |
| `user.created`, `user.deleted`, `user.tags.set` | `name` |
| `user.authentication.success`, `user.authentication.failure` | `name`, `auth_mechanism`, `peer_host`, `error` |
| `feature_flag.enabled` | `name` |

Events are delivered asynchronously; if more than 10,000 events are
pending (for example because a queue bound to the event exchange is
blocked) new events are dropped. Disable the exchange with
`event_exchange.enabled = false`.

## Firehose tracing

Tracing copies every message published to or delivered from a vhost to the
topic exchange `amq.rabbitmq.trace`:

```sh
tqctl -p /orders trace_on
tqctl -p /orders declare_queue firehose
tqctl -p /orders bind_queue amq.rabbitmq.trace firehose 'publish.#'
```

| Routing key | When |
|-------------|------|
| `publish.<exchange>` | a message is published to `<exchange>` |
| `deliver.<queue>` | a message is delivered from `<queue>` (to a consumer or `basic.get`) |

The trace message has the original body and the headers `exchange_name`,
`routing_keys`, `properties`, `node`, `vhost`, `user`, `connection`,
`channel` and, for publishes, `routed_queues`, for deliveries
`redelivered`. Tracing has a noticeable cost; switch it off with
`trace_off` when done.

## tq-top

`tq-top` shows the busiest queues and connections and refreshes every two
seconds:

```sh
tq-top -H mq-01 -u monitor -s rate
```

See [tools.md](tools.md#tq-top) for the options.

## Message interceptors

Interceptors stamp every message published by AMQP clients:

```ini
message_interceptors.incoming.set_header_timestamp.overwrite = false
message_interceptors.incoming.set_header_routing_node.overwrite = false
```

| Key | Adds |
|-----|------|
| `set_header_timestamp` | the `timestamp_in_ms` header (and the `timestamp` property, in seconds) with the time the broker received the message |
| `set_header_routing_node` | the `x-routed-by` header with the name of the node that routed the message |

With `overwrite = false` values set by the publisher are kept; with `true`
they are replaced. Leave the keys out to disable the interceptors.

## Logs

Logs go to standard error or to `log.file`. Each line has a timestamp, a
level and a category:

```
2026-03-02 10:15:22.481 [info] <connection> accepted AMQP connection 10.1.4.7:51874 -> 10.1.2.3:5672 (user 'billing', vhost '/')
```

Raise the level to `debug` temporarily to investigate client problems; it
logs every connection state change and protocol error in detail.
