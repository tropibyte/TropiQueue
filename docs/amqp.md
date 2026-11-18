# AMQP 0-9-1 support

TropiQueue implements AMQP 0-9-1 as specified, with the errata and
extensions published by RabbitMQ. Any client library written for RabbitMQ
can be used.

## Connection negotiation

| Step | Notes |
|------|-------|
| Protocol header | `AMQP\x00\x00\x09\x01`. Other headers receive the supported header and the socket is closed. |
| `connection.start` | Advertises `PLAIN AMQPLAIN` (see `auth_mechanisms`), the `en_US` locale and the capabilities below. |
| `connection.tune` | `channel_max` (2047), `frame_max` (128 KiB), `heartbeat` (60 s) from the configuration. Clients may lower but not raise them. |
| `connection.open` | The vhost must exist and the user needs permissions on it; vhost and user connection limits are checked here. |

Server capabilities:

| Capability | Meaning |
|-----------|---------|
| `publisher_confirms` | `confirm.select`, `basic.ack` / `basic.nack` to publishers |
| `exchange_exchange_bindings` | `exchange.bind` / `exchange.unbind` |
| `basic.nack` | `basic.nack` with `multiple` |
| `consumer_cancel_notify` | the server sends `basic.cancel` when a queue is deleted |
| `connection.blocked` | `connection.blocked` / `unblocked` during memory alarms |
| `consumer_priorities` | the `x-priority` consumer argument |
| `authentication_failure_close` | authentication failures are reported with `connection.close` (403) |
| `per_consumer_qos` | `basic.qos` with `global=false` applies per consumer |
| `direct_reply_to` | `amq.rabbitmq.reply-to` pseudo queue |

## Frames and limits

- Frames larger than the negotiated `frame_max` are a connection error
  (`FRAME_ERROR`).
- Message bodies above `max_message_size` (128 MB) are rejected with
  `PRECONDITION_FAILED`.
- Content header body sizes must match the bytes received.
- Heartbeats: a connection that sends nothing for two heartbeat intervals
  is closed; the broker sends heartbeats when it has nothing else to send.

## Methods

All methods of the classes `connection`, `channel`, `exchange`, `queue`,
`basic`, `confirm` and `tx` are implemented. Notes on behaviour:

### exchange.declare

- Names starting with `amq.` are reserved (`ACCESS_REFUSED`).
- Re-declaring an existing exchange with a different type, durability,
  auto-delete or internal flag fails with `PRECONDITION_FAILED`.
- `passive=true` only checks for existence.

### queue.declare

- An empty name asks the server for a unique `amq.gen-...` name.
- Re-declaring with different durability, exclusivity, auto-delete or
  queue arguments fails with `PRECONDITION_FAILED` (`inequivalent arg`).
- Exclusive queues are only visible to the declaring connection
  (`RESOURCE_LOCKED` for others).
- Arguments are validated: unknown `x-queue-type`, negative TTLs and
  lengths, invalid overflow modes are rejected.

### basic.publish

- Publishing to a missing exchange closes the channel with `NOT_FOUND`.
- Publishing to an internal exchange is refused (`ACCESS_REFUSED`).
- `mandatory=true` returns unroutable messages with `basic.return`
  (`NO_ROUTE`).
- `immediate=true` is not supported (as in RabbitMQ) and closes the
  connection with `NOT_IMPLEMENTED`.
- During a memory alarm the connection stops reading from publishing
  connections and sends `connection.blocked`.

### basic.consume / basic.qos

- `basic.qos` with `prefetch_count` limits unacknowledged deliveries per
  consumer (`global=false`) or per channel (`global=true`).
  A non-zero `prefetch_size` is refused with `NOT_IMPLEMENTED`.
- `exclusive=true` fails if the queue has other consumers, and other
  consumers are refused while it is active.
- `no_local` is accepted and ignored, as in RabbitMQ.
- `x-priority` and `x-stream-offset` / `x-stream-consumer-name` consumer
  arguments are supported.
- Consumers that hold a delivery longer than `consumer_timeout` cause the
  channel to be closed with `PRECONDITION_FAILED`, so that the message can
  be redelivered elsewhere.

### basic.ack / nack / reject / recover

- Delivery tags are channel scoped and increase by one per delivery.
  Acknowledging an unknown tag is a channel error (`PRECONDITION_FAILED -
  unknown delivery tag`).
- `multiple=true` covers every outstanding tag up to and including the
  given one.
- Requeued messages keep their original position in the queue and are
  marked `redelivered`.
- `basic.recover` with `requeue=true` returns every unacknowledged message
  on the channel to its queue.

### basic.get

- Delivers one message or `basic.get-empty`. The `message_count` field is
  the number of messages left in the queue.

### Publisher confirms

After `confirm.select` each publish gets a sequence number starting at 1.
The broker acks a message once every queue it was routed to has accepted it
(persistent messages on durable queues: once written to disk; quorum
queues: once committed). Unroutable messages are acked immediately (after
any `basic.return`). Queues that refuse a message (`reject-publish`
overflow) cause a `basic.nack`.

### Transactions

`tx.select`, `tx.commit` and `tx.rollback` buffer publishes and
acknowledgements until commit. Transactions are atomic per channel only
in the sense that nothing is applied before the commit; they are not
atomic across queues.

## Error codes

| Code | Name | Scope |
|------|------|-------|
| 311 | CONTENT_TOO_LARGE | channel |
| 312 | NO_ROUTE | return |
| 320 | CONNECTION_FORCED | connection |
| 402 | INVALID_PATH | connection |
| 403 | ACCESS_REFUSED | channel |
| 404 | NOT_FOUND | channel |
| 405 | RESOURCE_LOCKED | channel |
| 406 | PRECONDITION_FAILED | channel |
| 501 | FRAME_ERROR | connection |
| 502 | SYNTAX_ERROR | connection |
| 503 | COMMAND_INVALID | connection |
| 504 | CHANNEL_ERROR | connection |
| 505 | UNEXPECTED_FRAME | connection |
| 506 | RESOURCE_ERROR | connection |
| 530 | NOT_ALLOWED | connection |
| 540 | NOT_IMPLEMENTED | connection |
| 541 | INTERNAL_ERROR | connection |

## Direct reply-to

RPC clients can consume from the pseudo queue `amq.rabbitmq.reply-to` (in
no-ack mode) and publish requests with `reply_to: amq.rabbitmq.reply-to`.
The broker rewrites the property to a channel-specific address
(`amq.rabbitmq.reply-to.<token>`); replies published to the default
exchange with that routing key go straight to the requesting channel
without a queue.

## Field table types

The field table codec follows the RabbitMQ errata:

| Tag | Type | Tag | Type |
|-----|------|-----|------|
| `t` | boolean | `f` | float |
| `b` | int8 | `d` | double |
| `B` | uint8 | `D` | decimal |
| `s` | int16 | `S` | long string |
| `u` | uint16 | `x` | byte array |
| `I` | int32 | `A` | array |
| `i` | uint32 | `T` | timestamp |
| `l` | int64 | `F` | nested table |
| | | `V` | void |
