# Queues, streams and policies

TropiQueue has three queue types. The type is chosen when the queue is
declared with the `x-queue-type` argument (`classic`, the default,
`quorum` or `stream`) and cannot be changed afterwards.

| | classic | quorum | stream |
|---|---|---|---|
| Replication | no (one node) | Raft, majority commit | single node log |
| Durability | durable or transient | always durable | always durable |
| Consumption | destructive | destructive | non-destructive, replayable |
| Priorities | `x-max-priority` | no | no |
| Exclusive / auto-delete | yes | no | no |
| Dead lettering | yes | yes, with delivery limit | no |
| Typical use | work queues, RPC replies | data that must not be lost | event logs, fan-out to many readers, replay |

## Common arguments

| Argument | Policy key | Applies to | Meaning |
|----------|-----------|------------|---------|
| `x-message-ttl` | `message-ttl` | classic, quorum | Milliseconds a message may wait in the queue. |
| `x-expires` | `expires` | classic, quorum | Delete the queue after this many milliseconds without consumers or use. |
| `x-max-length` | `max-length` | classic, quorum | Maximum number of ready messages. |
| `x-max-length-bytes` | `max-length-bytes` | all | Maximum size of ready message bodies (for streams: retention). |
| `x-overflow` | `overflow` | classic, quorum | `drop-head` (default), `reject-publish` or `reject-publish-dlx`. |
| `x-dead-letter-exchange` | `dead-letter-exchange` | classic, quorum | Exchange that receives rejected, expired and dropped messages. |
| `x-dead-letter-routing-key` | `dead-letter-routing-key` | classic, quorum | Routing key used when dead lettering (default: the original). |
| `x-max-priority` | - | classic | Enables priorities 0..N (N at most 255; 10 is plenty). |
| `x-single-active-consumer` | `single-active-consumer` | classic, quorum | Only one consumer receives messages; the next takes over when it leaves. |
| `x-queue-mode` | `queue-mode` | classic | Accepted for compatibility; messages are always paged to the store when memory is tight. |
| `x-delivery-limit` | `delivery-limit` | quorum | Dead letter (or drop) a message after this many delivery attempts. |
| `x-quorum-initial-group-size` | - | quorum | Number of replicas (default: every cluster node). |
| `x-max-age` | `max-age` | stream | Retention by age: `7D`, `12h`, `30m`, `90s`, `1Y`. |
| `x-stream-max-segment-size-bytes` | `stream-max-segment-size-bytes` | stream | Segment file size (default 500 MB). |

When both a queue argument and a policy set a limit, the stricter one wins.
For other keys the declared argument takes precedence.

## Classic queues

Classic queues keep their messages in memory and, when durable, write
persistent messages to the shared message store and the queue's journal.
On restart the journal is replayed: acknowledged messages are dropped,
delivered-but-unacknowledged messages come back with `redelivered` set.

Exclusive queues belong to the declaring connection and are deleted when
it closes. Auto-delete queues are deleted when their last consumer is
cancelled. Server-named queues (`queue.declare` with an empty name) get a
name starting with `amq.gen-`.

### Priorities

With `x-max-priority` set, messages with a higher `priority` property are
delivered first; messages without the property have priority 0, and
priorities above the maximum are treated as the maximum.

### Consumer priorities and single active consumer

A consumer declared with the argument `x-priority` receives messages
before consumers with a lower priority as long as it has prefetch credit.
With `x-single-active-consumer` only one consumer is active at a time;
the others are on standby and the queue switches to the next one (in
subscription order) when the active consumer is cancelled or its channel
closes.

## Quorum queues

A quorum queue is a Raft group. Its replicas run on
`x-quorum-initial-group-size` nodes chosen when the queue is declared. The
leader accepts publishes and serves consumers; a message is confirmed to
the publisher once a majority of replicas have written it to their logs.
The queue stays available as long as a majority of its replicas are up.

Every replica applies the same commands in the same order, so all
decisions - which consumer gets which message, when a message expires,
when the delivery limit is reached - are made by the state machine from
the command log only. Each delivery attempt increments the
`x-delivery-count` header seen by consumers; with `x-delivery-limit` set,
a message returned more often than the limit is dead lettered instead of
redelivered, which protects consumers from poison messages.

## Streams

A stream is an append-only log. Consuming does not remove messages;
retention (`x-max-length-bytes`, `x-max-age`) removes whole segments once
they are entirely outside the limits.

AMQP consumers choose where to start with the `x-stream-offset` consumer
argument:

| Value | Start at |
|-------|----------|
| `first` | the oldest retained message |
| `last` | the last chunk written |
| `next` (default) | new messages only |
| a number | that offset |
| a timestamp (`T` value) | the first chunk written at or after that time (seconds) |
| `"10m"`, `"2h"` | messages written in the last interval |

Every delivery carries its offset in the `x-stream-offset` header. Stream
consumers must use manual acknowledgement and a prefetch count; acks are
used for flow control only.

A consumer that sets `x-stream-consumer-name` has its position tracked on
the server: acknowledgements store the offset and the next subscription
with the same name resumes after it.

The [stream protocol](stream-protocol.md) offers higher throughput for the
same streams; messages written with one protocol can be read with the
other.

### Super streams

A super stream is a partitioned stream: an exchange named after the super
stream routes to `N` partition streams named `<name>-0` ... `<name>-N-1`.

```sh
tqctl add_super_stream invoices 3
```

Producers publish to the exchange; consumers read one or more partitions.
Stream protocol clients discover the partitions with the `partitions`
command and the target partition of a routing key with `route`.

## Dead lettering

A message is dead lettered when it is rejected or nacked with
`requeue=false`, expires, is dropped from the head of a full queue, or
exceeds a quorum queue's delivery limit. It is republished to the
dead-letter exchange with the original or configured routing key and an
`x-death` header that records the history:

```
x-death: [{queue: "orders", reason: "rejected", count: 1, exchange: "", routing-keys: ["orders"], time: ...}]
x-first-death-queue: orders
x-first-death-reason: rejected
x-first-death-exchange: ""
```

Messages that would cycle back to the queue they were dead lettered from
without any rejection are dropped to avoid infinite loops.

## Exchanges

| Type | Routing |
|------|---------|
| `direct` | routing key equals binding key |
| `fanout` | every bound queue |
| `topic` | dotted words; `*` matches one word, `#` zero or more |
| `headers` | `x-match: all` (default) or `any` over binding arguments |
| `x-consistent-hash` | hash of the routing key (or the `hash-header` header) over bindings weighted by their routing key |
| `x-delayed-message` | like `x-delayed-type`, after the delay in the `x-delay` header (milliseconds) |
| `x-local-random` | one bound queue chosen at random per message; the routing key is ignored |

The `alternate-exchange` argument (or policy key) names an exchange that
receives messages the exchange could not route. The `CC` and `BCC`
headers add extra routing keys; `BCC` is removed before delivery.

## Policies

Policies apply arguments to queues and exchanges whose names match a
regular expression, so they can be changed without redeclaring:

```sh
tqctl set_policy ttl '^tmp\.' '{"message-ttl": 60000, "expires": 600000}' 1 queues
```

| Field | Meaning |
|-------|---------|
| `pattern` | regular expression matched against the full name |
| `apply-to` | `all`, `queues`, `exchanges`, `classic_queues`, `quorum_queues` or `streams` |
| `priority` | when several policies match, the highest priority wins |
| `definition` | the keys from the table above, plus `alternate-exchange` for exchanges and the federation keys `federation-upstream` / `federation-upstream-set` |

Changing or removing a policy re-applies policies to existing queues.
