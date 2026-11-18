# Examples

Small programs that show how to use the TropiQueue client library
(`src/client`). They are built with the rest of the project unless
`-DTQ_BUILD_EXAMPLES=OFF` is passed to CMake, and connect to
`amqp://guest:guest@localhost:5672/%2F` unless a URI is given as the first
argument (or in the `TQ_URI` environment variable).

| Program | Shows |
|---------|-------|
| `hello_publisher` | connecting, declaring a queue, publishing a persistent message |
| `work_queue_worker` | a competing consumer with prefetch, manual acks and automatic recovery |
| `topic_logs` | publish/subscribe with a topic exchange and a server-named queue |
| `rpc_server`, `rpc_client` | request/reply with direct reply-to |
| `confirms_batch` | publisher confirms in batches, with retry of unconfirmed batches |
| `stream_tail` | reading a stream from an offset with `x-stream-offset` |

Try the work queue:

```sh
./work_queue_worker &          # start one or more workers
./hello_publisher amqp://guest:guest@localhost/%2F "task 1"
```

Or request/reply:

```sh
./rpc_server &
./rpc_client 30                # asks the server for fib(30)
```
