# The stream protocol

Streams can be used over AMQP 0-9-1, but publishing and consuming one
message per frame limits throughput. The binary stream protocol, enabled
with `stream.enabled = true` (port 5552), moves batches of messages per
frame and delivers whole chunks under credit-based flow control. It is
wire-compatible with version 1 of the RabbitMQ stream protocol for the
commands listed below, so existing stream client libraries can connect.

## Frames

```
frame    = size:uint32  key:uint16  version:uint16  payload
response = key | 0x8000, correlation-id:uint32, response-code:uint16, ...
string   = length:int16  utf-8 bytes        (-1 = null)
bytes    = length:int32  bytes
array    = count:int32   items
map      = count:int32   (key:string value:string)*
```

`size` counts everything after itself. Before authentication frames are
limited to 64 KiB; after `tune` to the negotiated frame max
(`stream.frame_max`, 1 MiB by default).

## Connecting

```
client                               server
  peer_properties(corr, props)  ->
                                <-   peer_properties response (server props)
  sasl_handshake(corr)          ->
                                <-   mechanisms ["PLAIN"]
  sasl_authenticate(corr, "PLAIN", "\0user\0password") ->
                                <-   ok
                                <-   tune(frame_max, heartbeat)
  tune(frame_max, heartbeat)    ->
  open(corr, vhost)             ->
                                <-   ok + {advertised_host, advertised_port}
```

The negotiated frame max and heartbeat are the minimum of both sides'
values (0 means "no limit" / "no heartbeat"). OAuth 2.0 access tokens are
accepted as the `PLAIN` password.

## Commands

| Key | Command | Direction | Notes |
|-----|---------|-----------|-------|
| 0x01 | declare_publisher | C -> S | `corr, publisher-id:uint8, reference:string, stream:string`; needs write permission |
| 0x02 | publish | C -> S | `publisher-id, count:int32, (publishing-id:uint64, message:bytes)*` |
| 0x03 | publish_confirm | S -> C | `publisher-id, count, publishing-id*` |
| 0x04 | publish_error | S -> C | `publisher-id, count, (publishing-id, code:uint16)*` |
| 0x05 | query_publisher_sequence | C -> S | `corr, reference, stream` -> `sequence:uint64` |
| 0x06 | delete_publisher | C -> S | `corr, publisher-id` |
| 0x07 | subscribe | C -> S | `corr, subscription-id:uint8, stream, offset-spec, credit:uint16, properties:map`; needs read permission |
| 0x08 | deliver | S -> C | `subscription-id, chunk` |
| 0x09 | credit | C -> S | `subscription-id, credit:uint16`; errors are answered with a credit response |
| 0x0a | store_offset | C -> S | `reference, stream, offset:uint64` (no response) |
| 0x0b | query_offset | C -> S | `corr, reference, stream` -> `offset:uint64` or `no_offset` |
| 0x0c | unsubscribe | C -> S | `corr, subscription-id` |
| 0x0d | create | C -> S | `corr, stream, arguments:map`; needs configure permission |
| 0x0e | delete | C -> S | `corr, stream`; needs configure permission |
| 0x0f | metadata | C -> S | `corr, streams:array` -> brokers and per-stream leader |
| 0x11 | peer_properties | C -> S | handshake |
| 0x12 | sasl_handshake | C -> S | handshake |
| 0x13 | sasl_authenticate | C -> S | handshake |
| 0x14 | tune | both | handshake |
| 0x15 | open | C -> S | handshake |
| 0x16 | close | both | `corr, code:uint16, reason:string` |
| 0x17 | heartbeat | both | no payload |
| 0x18 | route | C -> S | `corr, routing-key, super-stream` -> streams |
| 0x19 | partitions | C -> S | `corr, super-stream` -> streams in partition order |
| 0x1c | stream_stats | C -> S | `corr, stream` -> map of int64 statistics |

`create` accepts the arguments `max-length-bytes`, `max-age`,
`stream-max-segment-size-bytes`, `queue-leader-locator` and
`initial-cluster-size`; anything else is refused with
`precondition_failed`.

## Offset specifications

| Value | Type | Extra field | Starts at |
|-------|------|-------------|-----------|
| 1 | first | - | the oldest retained message |
| 2 | last | - | the last message written |
| 3 | next | - | new messages only |
| 4 | offset | uint64 | the given offset (or the first retained one) |
| 5 | timestamp | int64 (ms) | the first chunk written at or after the time |

## Deduplication

A publisher declared with a non-empty `reference` is a named publisher.
The broker remembers the highest publishing id stored for each
(stream, reference) pair and drops messages whose id is not greater than
it, confirming them without storing them again. After reconnecting, a
client queries `query_publisher_sequence` and resumes from the next id.
Publishing ids must therefore increase strictly per named publisher.

## Chunks

`deliver` carries one chunk:

```
magic-version:uint8 = 0x50, chunk-type:uint8, num-entries:uint16,
num-records:uint32, timestamp:int64, epoch:uint64,
chunk-first-offset:uint64, chunk-crc:int32 (CRC-32C of the data),
data-length:uint32, trailer-length:uint32, reserved:uint32,
data = (0:1 size:31 bits, message bytes)*
```

Each `deliver` consumes one credit. Clients usually subscribe with a
credit of 10 or more and return one credit per chunk they have processed.

## Message format

Message bodies are stored as opaque bytes: a stream client receives
exactly the bytes it published, and AMQP 0-9-1 consumers of the same
stream receive them as the message body. (RabbitMQ stores AMQP 1.0 encoded
messages; clients that rely on AMQP 1.0 properties need to encode them in
the body.)

## Response codes

| Code | Meaning |
|------|---------|
| 1 | ok |
| 2 | stream does not exist |
| 3 | subscription id already exists |
| 4 | subscription id does not exist |
| 5 | stream already exists |
| 6 | stream not available |
| 7 | SASL mechanism not supported |
| 8 | authentication failure |
| 9 | SASL error |
| 11 | authentication failure (loopback only user) |
| 12 | virtual host access failure |
| 13 | unknown frame |
| 14 | frame too large |
| 15 | internal error |
| 16 | access refused |
| 17 | precondition failed |
| 18 | publisher does not exist |
| 19 | no offset |
