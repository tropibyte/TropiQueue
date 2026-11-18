# Changelog

All notable changes to TropiQueue are documented here. The project follows
semantic versioning; until 1.0 minor releases may change configuration
keys and the management API, which is called out below.

## 0.9.4

### Added

- Binary stream protocol listener (`stream.enabled`, port 5552) compatible
  with RabbitMQ stream clients: publisher deduplication, credit-based
  delivery, offset tracking, metadata, super stream partitions and routes.
- Feature flags (`/api/feature-flags`, `tqctl list_feature_flags`,
  `enable_feature_flag`).
- `/metrics/detailed` with per-family, per-vhost metrics (experimental,
  behind the `detailed_queue_metrics` feature flag).
- Port and protocol listener health checks.
- `tq-top`, a live view of queues and connections.
- `x-local-random` exchange type.
- Incoming message interceptors (`timestamp_in_ms`, `x-routed-by`).
- `load_definitions`: import a definitions file at every start.
- Process-wide Prometheus counters for publishes, deliveries, acks,
  confirms, connection churn and store sync latency.

### Changed

- The client library URL-decodes the vhost of AMQP URIs, so
  `amqp://host/%2F` selects the default vhost.
- `client::Connection::close()` may be called from several threads.

## 0.9.3

### Added

- OAuth 2.0 access tokens (JWT, HS256) for AMQP, MQTT, STOMP and the
  management API, with scope-based permissions and
  `connection.update-secret`.
- Dynamic shovels and exchange federation, configured through runtime
  parameters.
- Runtime and global parameters API; `cluster_name` global parameter.
- Internal event exchange `amq.rabbitmq.event` and firehose tracing
  (`amq.rabbitmq.trace`).
- Per-user `max-connections` and `max-channels` limits.
- Paging, filtering and sorting of management API lists.
- Web management UI served at `/`.
- Configuration schema: unknown keys are reported with suggestions and
  invalid values stop the broker (`tqd --check-config`,
  `--sample-config`, `--list-config-keys`).
- Automatic connection and topology recovery in the client library.

### Changed

- **Breaking:** invalid configuration values are now fatal at start-up
  instead of falling back to the default with a warning.

## 0.9.2

### Added

- MQTT 3.1.1 adapter with retained messages, wills and persistent
  sessions.
- STOMP 1.2 adapter.
- Super streams (`PUT /api/super-streams/{vhost}/{name}`).
- `tq-dump raft` to inspect Raft groups.

### Fixed

- Quorum queue leaders now use the time recorded in the log for message
  expiry after a leader change.

## 0.9.1

### Added

- Streams: segment log with retention by size and age, offset and
  timestamp replay, named consumer offset tracking.
- Delayed message exchange (`x-delayed-message`).
- `tqctl export_definitions` / `import_definitions`.

## 0.9.0

First public preview: AMQP 0-9-1 broker with classic and quorum queues,
Raft clustering, the management HTTP API, Prometheus metrics, `tqctl` and
`tq-perf`.
