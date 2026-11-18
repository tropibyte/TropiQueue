# MQTT and STOMP

The MQTT and STOMP adapters map their protocols onto the AMQP model, so
clients of every protocol can exchange messages. Both use the broker's
users and permissions.

## MQTT 3.1.1

Enable the listener:

```ini
mqtt.enabled = true
mqtt.listeners.tcp.default = 1883
mqtt.vhost = /
mqtt.allow_anonymous = false
```

### Topics and routing

MQTT clients publish to and subscribe from the `amq.topic` exchange of
`mqtt.vhost`. Topic names are converted to AMQP routing keys:

| MQTT | AMQP |
|------|------|
| `/` level separator | `.` |
| `+` single-level wildcard | `*` |
| `#` multi-level wildcard | `#` |
| `.` inside a level | `/` |

So an MQTT subscription to `sensors/+/temperature` receives messages that
AMQP clients publish to `amq.topic` with the routing key
`sensors.kitchen.temperature`, and vice versa.

### Sessions and subscriptions

Each subscription creates a queue named
`mqtt-subscription-<client id>qos<0|1>` bound to `amq.topic`. With
`clean_session = 1` the queues are exclusive and deleted on disconnect;
with `clean_session = 0` they are durable and keep collecting messages
while the client is away, and the client resumes them when it reconnects
with the same client id. A second connection with an existing client id
takes over the session and the older connection is closed.

### Quality of service

- QoS 0: at most once; deliveries are not acknowledged.
- QoS 1: at least once; the broker resends unacknowledged `PUBLISH`es with
  the DUP flag after a reconnect, and acknowledges client publishes with
  `PUBACK` once routed.
- QoS 2 requests are downgraded to QoS 1.

### Retained messages and wills

Retained messages are kept per topic (an empty retained payload clears
the topic) and sent to new subscribers whose filter matches. The last will
is published when a client disconnects without sending `DISCONNECT` or
misses its keep-alive (1.5 times the keep-alive interval).

### Authentication

MQTT `CONNECT` carries the user name and password; OAuth 2.0 access tokens
are accepted as the password. With `mqtt.allow_anonymous = true` clients
without credentials connect as the default user. Publishing needs write
permission on `amq.topic` (and topic permission for the routing key);
subscribing needs read permission on `amq.topic` and configure/write/read
on the subscription queue.

## STOMP 1.2

Enable the listener:

```ini
stomp.enabled = true
stomp.listeners.tcp.default = 61613
```

`CONNECT` / `STOMP` frames use the `login` and `passcode` headers and the
`host` header for the vhost (the default vhost when omitted).

### Destinations

| Destination | Meaning |
|-------------|---------|
| `/queue/<name>` | The durable queue `<name>`, declared on first use. `SEND` publishes through the default exchange. |
| `/amq/queue/<name>` | An existing queue; nothing is declared. |
| `/topic/<key>` | `amq.topic` with routing key `<key>`; each `SUBSCRIBE` gets its own exclusive queue. |
| `/exchange/<exchange>/<key>` | Any exchange; subscriptions bind an exclusive queue with `<key>`. |

### Frames

| Frame | Notes |
|-------|-------|
| `SEND` | Headers other than `destination`, `receipt`, `content-length` and `transaction` become message headers; `content-type`, `persistent`, `priority`, `reply-to`, `correlation-id`, `expires` map to AMQP properties. |
| `SUBSCRIBE` | `ack: auto` (default), `client` or `client-individual`; `prefetch-count` limits unacknowledged messages. |
| `ACK` / `NACK` | Acknowledge or reject (requeue) by the `ack` header of the `MESSAGE`. |
| `UNSUBSCRIBE` | Cancels the subscription and removes its exclusive queue. |
| `BEGIN` / `COMMIT` / `ABORT` | Accepted; transactions are not atomic (frames are applied immediately). |
| `DISCONNECT` | With `receipt`, the receipt is sent before closing. |

Any frame with a `receipt` header is answered with `RECEIPT`. Errors are
reported with an `ERROR` frame and the connection is closed.

Header values are escaped as required by STOMP 1.2 (`\n`, `\c`, `\\`,
`\r`).
