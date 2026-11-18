# The C++ client library

`tq_client` is a small, blocking AMQP 0-9-1 client used by the tools, the
shovel and federation code and the test-suite. It works with TropiQueue and
with other AMQP 0-9-1 brokers. Link against `tq_client` and include the
headers from `src/client`.

## Connecting

```cpp
#include "client/channel.h"
#include "client/connection.h"

std::string error;
auto conn = tq::client::Connection::open("amqp://app:secret@mq-01:5672/orders?heartbeat=30", &error);
if (!conn) {
    std::cerr << "cannot connect: " << error << "\n";
    return 1;
}
auto ch = conn->openChannel();
```

`Connection::open` also accepts a `ConnectionParams` struct. URI query
parameters:

| Parameter | Meaning |
|-----------|---------|
| `heartbeat` | heartbeat interval in seconds (0 disables) |
| `frame_max` | maximum frame size to request |
| `channel_max` | maximum channel number to request |
| `connection_timeout` | TCP connect timeout in milliseconds |
| `name` | connection name shown in the management UI |
| `auth_mechanism` | `PLAIN` (default) or `AMQPLAIN` |

User name, password and vhost are URL-decoded, so the default vhost is
written `%2F` and special characters in passwords must be percent-encoded.

The connection runs a reader thread that dispatches frames to channels
and, when heartbeats are enabled, a heartbeat thread. `close()` (also
called by the destructor) performs the AMQP close handshake.

## Channels

Every method blocks until the broker replies and throws
`tq::client::ClientError` (with the AMQP reply code in `code`) when the
broker closes the channel or connection.

```cpp
ch->declareExchange("orders", "topic");
auto q = ch->declareQueue("orders.eu", true, false, false, args);
ch->bindQueue(q.name, "orders", "eu.#");

amqp::BasicProperties props;
props.deliveryMode = 2;
props.contentType = "application/json";
ch->confirmSelect();
ch->publish("orders", "eu.created", R"({"id": 42})", props);
if (!ch->waitForConfirms(5000)) {
    // timed out or nacked: retry or report
}
```

### Consuming

```cpp
ch->qos(50);
std::string tag = ch->consume("orders.eu", [&](const tq::client::Delivery& d) {
    handle(d.body);
    ch->ack(d.deliveryTag);
});
```

The callback runs on the connection's reader thread: keep it short, or
hand the delivery to a worker. `ack`, `nack` and `reject` only send a frame
and may be called from the callback; blocking calls (declarations,
`waitForConfirms`) must not be made from it.

`get()` fetches a single message and returns `std::nullopt` when the queue
is empty.

### Returned messages

```cpp
ch->onReturn([](const tq::client::ReturnedMessage& r) {
    std::cerr << "unroutable: " << r.routingKey << " (" << r.replyText << ")\n";
});
ch->publish("orders", "unknown.key", body, props, /*mandatory=*/true);
```

## Automatic recovery

`RecoveringConnection` reconnects after network failures, broker restarts
and forced closes, and restores the channels and topology created through
it:

```cpp
#include "client/recovery.h"

tq::client::RecoveryOptions opts;
opts.networkRecoveryIntervalMs = 2000;
auto rc = tq::client::RecoveringConnection::open(params, opts, &error);
rc->onConnectionLost([](const std::string& why) { log("lost: " + why); });
rc->onRecovered([] { log("recovered"); });

auto ch = rc->openChannel();
ch->declareQueue("work", true);
ch->qos(10);
ch->consume("work", [&](const tq::client::Delivery& d) {
    process(d);
    ch->ack(d.deliveryTag);
});
```

Recovery reopens each channel, re-applies `qos` and confirm mode,
re-declares the recorded exchanges, queues and bindings (unless
`topologyRecovery` is false) and restarts consumers with their original
consumer tags. Server-named queues receive a new name.

Delivery tags remain increasing across a recovery: the channel adds an
offset to the tags of new deliveries. Acknowledgements for deliveries made
before the connection was lost are ignored, because the broker requeued
those messages when the old connection closed - they will be delivered
again.

## RPC helpers

`RpcServer` consumes requests from a queue and replies to the `reply_to`
address with the same `correlation_id`. `RpcClient` publishes requests and
waits for the matching reply, using direct reply-to by default:

```cpp
tq::client::RpcServer server(conn, "pricing", [](const tq::client::Delivery& d) {
    return computePrice(d.body);
});

tq::client::RpcClient client(conn2);
auto reply = client.call("", "pricing", R"({"sku": "A-1"})", 2000);
```

`call` returns `std::nullopt` on timeout.

## Thread safety

A `Connection` may be shared between threads. A `Channel` must not be used
by several threads at once (as in other AMQP clients); open one channel per
thread instead.
