# Contributing to TropiQueue

Thank you for helping. This page explains how to build, test and submit
changes.

## Building and testing

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Run a single suite or test while iterating:

```sh
./build/tq_tests --list
./build/tq_tests --suite amqp_confirms
./build/tq_tests --suite amqp_confirms --test nack_on_reject_publish
```

Before sending a change, also build with the sanitizers:

```sh
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DTQ_SANITIZE=ON
cmake --build build-asan -j && ctest --test-dir build-asan
```

and, if you touched platform code (`src/net`, `src/common/fileio.cpp`,
`tools/tqd`), make sure Windows still builds - either with Visual Studio or
with a MinGW-w64 cross toolchain:

```sh
cmake -S . -B build-mingw -DCMAKE_TOOLCHAIN_FILE=cmake/mingw-w64.cmake -DTQ_BUILD_TESTS=OFF
cmake --build build-mingw -j
```

## Code style

- C++20, no compiler extensions, no third-party dependencies.
- Formatting follows `.clang-format` (4 spaces, 120 columns).
- Types are `PascalCase`, functions and variables `camelCase`, members end
  with `_`, constants start with `k`.
- Every file starts with the two-line project header.
- Prefer RAII and value types; raw `new`/`delete` should not appear.
- Locks protect data, not code: document what each mutex guards next to
  its declaration, and never call out to other subsystems (queues, vhosts,
  callbacks) while holding a lock unless the lock order is documented.
- Anything parsed from the network or from disk is untrusted: check sizes
  before allocating and lengths before reading.

## Tests

Every behaviour change needs a test. Unit tests live next to the area they
cover (`tests/test_codec.cpp`, `tests/test_routing.cpp`, ...); end-to-end
tests start a broker with `BrokerFixture` and talk to it with the client
library or over HTTP. Tests must be deterministic: wait for conditions with
`waitUntil` instead of sleeping, and use ephemeral ports.

New test suites must be added to `TQ_TEST_SUITES` in `CMakeLists.txt` so
CTest runs them.

## Commit messages

Use the imperative mood ("Add delivery limit to quorum queues"), keep the
first line under 72 characters, and explain the why in the body.

## Licensing

By contributing you agree that your contributions are licensed under the
MIT License of the project.
