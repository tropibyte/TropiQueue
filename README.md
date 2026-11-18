# TropiQueue

TropiQueue is a message broker written in portable C++20. It speaks
AMQP 0-9-1 and is designed as a drop-in alternative to RabbitMQ for the
large majority of applications: existing AMQP client libraries, management
scripts and definition files work unchanged.

It ships as a single self-contained binary with no third-party dependencies
and runs on Linux and Windows.

## Highlights

- **AMQP 0-9-1** with the RabbitMQ extensions applications rely on:
  publisher confirms, consumer cancel notifications, `basic.nack`,
  exchange-to-exchange bindings, sender-selected distribution, direct
  reply-to (`amq.rabbitmq.reply-to`), connection blocking and
  authentication failure notifications.
- **Exchanges**: direct, fanout, topic, headers, consistent hash
  (`x-consistent-hash`) and delayed message (`x-delayed-message`), plus
  alternate exchanges.
- **Three queue types**
  - *classic* queues with priorities, TTLs, length limits, dead lettering,
    single active consumer and durable storage;
  - *quorum* queues replicated with Raft for data safety, with delivery
    limits and poison message handling;
  - *streams*, append-only logs with offset/timestamp replay, retention by
    size and age, named consumer offset tracking and super streams.
- **Stream protocol**: a binary protocol (port 5552) compatible with the
  RabbitMQ stream protocol for high-throughput publishing and consuming.
- **MQTT 3.1.1** and **STOMP 1.2** adapters.
- **Clustering** through Raft: replicated metadata, quorum queues and
  queue placement across nodes.
- **Dynamic shovels** and **exchange federation** to move messages between
  brokers and data centres.
- **Security**: users with tags, per-vhost regex permissions, topic
  permissions, loopback-only users, per-user and per-vhost limits, and
  OAuth 2.0 access tokens (JWT).
- **Operations**: a RabbitMQ-compatible management HTTP API, a web UI,
  Prometheus metrics, health checks, internal event exchange, firehose
  tracing, feature flags and definitions import/export.
- **Tools**: `tqd` (the broker), `tqctl` (administration), `tq-perf`
  (load generator), `tq-top` (live view) and `tq-dump` (data inspection),
  plus [example programs](examples/README.md) for the client library.

## Building

TropiQueue needs CMake 3.16+ and a C++20 compiler (GCC 11+, Clang 14+ or
Visual Studio 2022).

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Useful options:

| Option | Default | Meaning |
|--------|---------|---------|
| `TQ_BUILD_TESTS` | `ON` | Build the test-suite (`tq_tests`). |
| `TQ_BUILD_TOOLS` | `ON` | Build `tqd`, `tqctl`, `tq-perf`, `tq-top` and `tq-dump`. |
| `TQ_BUILD_EXAMPLES` | `ON` | Build the client library examples. |
| `TQ_SANITIZE` | `OFF` | Build with AddressSanitizer and UndefinedBehaviorSanitizer. |
| `TQ_WARNINGS_AS_ERRORS` | `OFF` | Treat compiler warnings as errors. |

On Windows, open the folder in Visual Studio or run the same CMake
commands from a developer prompt. MinGW-w64 cross builds are supported as
well.

## Running

```sh
./build/tqd --config config/tropiqueue.conf
```

With no configuration file the broker listens for AMQP on port 5672 and
serves the management API and web UI on port 15672. On first start it
creates the virtual host `/` and the user `guest` (password `guest`), which
can only connect from localhost.

Any configuration key can be overridden on the command line:

```sh
./build/tqd --listeners.tcp.default=5673 --management.tcp.port=15673 --log.level=debug
```

`tqd --check-config` validates a configuration file, `tqd --sample-config`
prints every key with its default, and `tqd --list-config-keys` prints the
key reference shown in [docs/configuration.md](docs/configuration.md).

Then open <http://localhost:15672/> or use `tqctl`:

```sh
./build/tqctl add_user app 'a-strong-password' management
./build/tqctl set_permissions app '.*' '.*' '.*'
./build/tqctl declare_queue orders quorum
./build/tqctl list_queues
```

## Documentation

| Topic | Document |
|-------|----------|
| How the broker is put together | [docs/architecture.md](docs/architecture.md) |
| Configuration keys | [docs/configuration.md](docs/configuration.md) |
| AMQP 0-9-1 support and extensions | [docs/amqp.md](docs/amqp.md) |
| Queue types, policies and arguments | [docs/queues.md](docs/queues.md) |
| The binary stream protocol | [docs/stream-protocol.md](docs/stream-protocol.md) |
| Users, permissions and OAuth 2.0 | [docs/security.md](docs/security.md) |
| Clustering | [docs/clustering.md](docs/clustering.md) |
| Shovels and federation | [docs/shovel-federation.md](docs/shovel-federation.md) |
| MQTT and STOMP | [docs/mqtt-stomp.md](docs/mqtt-stomp.md) |
| Management HTTP API | [docs/management-api.md](docs/management-api.md) |
| Monitoring, events and tracing | [docs/monitoring.md](docs/monitoring.md) |
| The C++ client library | [docs/client-library.md](docs/client-library.md) |
| Command line tools | [docs/tools.md](docs/tools.md) |
| Performance tuning | [docs/performance.md](docs/performance.md) |
| Troubleshooting | [docs/troubleshooting.md](docs/troubleshooting.md) |
| Migrating from RabbitMQ | [docs/migrating-from-rabbitmq.md](docs/migrating-from-rabbitmq.md) |

## Compatibility notes

TropiQueue follows RabbitMQ's behaviour wherever applications can observe
it, including reply codes, error texts, header names (`x-death`,
`x-delivery-count`, `x-stream-offset`, ...) and the JSON shapes of the
management API. Known differences:

- AMQP 1.0 is not implemented. Stream protocol message bodies are stored as
  opaque bytes rather than AMQP 1.0 encoded messages.
- TLS is not built in; terminate TLS in a proxy (HAProxy, Envoy, stunnel)
  in front of the listeners.
- The management UI covers day-to-day operations but not every page of the
  RabbitMQ UI.
- Queue federation is not supported; use exchange federation or shovels.

## License

TropiQueue is released under the MIT License. See [LICENSE](LICENSE).

Copyright (c) Tropibyte, Inc.
