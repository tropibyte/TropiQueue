cmake_minimum_required(VERSION 3.16)

project(TropiQueue VERSION 0.9.4 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(TQ_BUILD_TESTS "Build the test suite" ON)
option(TQ_BUILD_EXAMPLES "Build the client library examples" ON)
option(TQ_BUILD_TOOLS "Build tqd, tqctl, tq-perf, tq-top and tq-dump" ON)
option(TQ_SANITIZE "Build with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(TQ_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

find_package(Threads REQUIRED)

if(MSVC)
  add_compile_options(/W4 /permissive- /utf-8 /EHsc /bigobj)
  add_compile_definitions(_CRT_SECURE_NO_WARNINGS _WIN32_WINNT=0x0601)
  if(TQ_WARNINGS_AS_ERRORS)
    add_compile_options(/WX)
  endif()
else()
  add_compile_options(-Wall -Wextra -Wpedantic -Wno-unused-parameter)
  if(TQ_WARNINGS_AS_ERRORS)
    add_compile_options(-Werror)
  endif()
  if(TQ_SANITIZE)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
  endif()
endif()

if(WIN32)
  add_compile_definitions(_WIN32_WINNT=0x0601)
endif()

# ---------------------------------------------------------------- libraries

set(TQ_COMMON_SOURCES
  src/common/bytes.cpp
  src/common/config.cpp
  src/common/config_schema.cpp
  src/common/crypto.cpp
  src/common/executor.cpp
  src/common/fileio.cpp
  src/common/json.cpp
  src/common/logging.cpp
  src/common/metrics.cpp
  src/common/random.cpp
  src/common/strings.cpp
  src/net/http.cpp
  src/net/socket.cpp
  src/net/tcp_server.cpp
  src/amqp/constants.cpp
  src/amqp/field_table.cpp
  src/amqp/frame.cpp
  src/amqp/methods.cpp
  src/amqp/properties.cpp
)

add_library(tq_common STATIC ${TQ_COMMON_SOURCES})
target_include_directories(tq_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(tq_common PUBLIC Threads::Threads)
if(WIN32)
  target_link_libraries(tq_common PUBLIC ws2_32)
endif()

add_library(tq_client STATIC
  src/client/connection.cpp
  src/client/channel.cpp
  src/client/uri.cpp
  src/client/rpc.cpp
  src/client/recovery.cpp
)
target_link_libraries(tq_client PUBLIC tq_common)

add_library(tq_broker STATIC
  src/broker/broker.cpp
  src/broker/broker_metrics.cpp
  src/broker/channel.cpp
  src/broker/classic_queue.cpp
  src/broker/connection.cpp
  src/broker/events.cpp
  src/broker/exchange.cpp
  src/broker/feature_flags.cpp
  src/broker/message.cpp
  src/broker/parameters.cpp
  src/broker/policy.cpp
  src/broker/queue.cpp
  src/broker/quorum_queue.cpp
  src/broker/remote_queue.cpp
  src/broker/stream_queue.cpp
  src/broker/vhost.cpp
  src/cluster/cluster.cpp
  src/cluster/quorum_state.cpp
  src/cluster/raft.cpp
  src/cluster/transport.cpp
  src/federation/federation.cpp
  src/mgmt/management.cpp
  src/mgmt/routes.cpp
  src/mgmt/routes_ext.cpp
  src/mqtt/mqtt_codec.cpp
  src/mqtt/mqtt_server.cpp
  src/security/jwt.cpp
  src/shovel/shovel.cpp
  src/security/sasl.cpp
  src/security/users.cpp
  src/storage/message_store.cpp
  src/storage/metadata_store.cpp
  src/storage/queue_store.cpp
  src/storage/stream_log.cpp
  src/stomp/stomp_server.cpp
  src/streamproto/stream_codec.cpp
  src/streamproto/stream_server.cpp
)
target_link_libraries(tq_broker PUBLIC tq_common tq_client)

# -------------------------------------------------------------------- tools

if(TQ_BUILD_TOOLS)
  add_executable(tqd tools/tqd/main.cpp)
  target_link_libraries(tqd PRIVATE tq_broker)

  add_executable(tqctl tools/tqctl/main.cpp)
  target_link_libraries(tqctl PRIVATE tq_common)

  add_executable(tq-perf tools/tq-perf/main.cpp)
  target_link_libraries(tq-perf PRIVATE tq_client)

  add_executable(tq-top tools/tq-top/main.cpp)
  target_link_libraries(tq-top PRIVATE tq_common)

  add_executable(tq-dump tools/tq-dump/main.cpp)
  target_link_libraries(tq-dump PRIVATE tq_broker)
endif()

# ----------------------------------------------------------------- examples

if(TQ_BUILD_EXAMPLES)
  foreach(example hello_publisher work_queue_worker topic_logs rpc_server rpc_client confirms_batch stream_tail)
    add_executable(${example} examples/${example}.cpp)
    target_link_libraries(${example} PRIVATE tq_client)
    target_include_directories(${example} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/examples)
  endforeach()
endif()

# -------------------------------------------------------------------- tests

if(TQ_BUILD_TESTS)
  enable_testing()
  file(GLOB TQ_TEST_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp)
  add_executable(tq_tests ${TQ_TEST_SOURCES})
  target_link_libraries(tq_tests PRIVATE tq_broker tq_client)
  target_include_directories(tq_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
  target_compile_definitions(tq_tests PRIVATE TQ_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")

  set(TQ_TEST_SUITES
    bytes strings crypto json config executor fileio metrics
    field_table frame methods properties
    exchange topic classic_queue policy users sasl
    message_store queue_store stream_log metadata_store
    raft quorum_state
    amqp_connection amqp_publish_consume amqp_confirms amqp_tx amqp_errors amqp_dlx amqp_priority
    amqp_streams amqp_quorum amqp_exclusive amqp_reply_to
    management http client_uri recovery mqtt stomp
    jwt parameters config_schema shovel federation
    events tracing user_limits oauth2 management_ext client_recovery
    stream_codec stream_protocol feature_flags
  )
  foreach(suite ${TQ_TEST_SUITES})
    add_test(NAME ${suite} COMMAND tq_tests --suite ${suite})
    set_tests_properties(${suite} PROPERTIES TIMEOUT 120)
  endforeach()
endif()
