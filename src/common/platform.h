// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#if defined(_WIN32)
#  ifndef TQ_WINDOWS
#    define TQ_WINDOWS 1
#  endif
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#else
#  define TQ_POSIX 1
#endif

#include <cstdint>
#include <cstddef>

namespace tq {

constexpr const char* kProductName = "TropiQueue";
constexpr const char* kProductVersion = "0.9.4";
constexpr const char* kCopyright = "Copyright (c) Tropibyte, Inc.";

#if defined(TQ_WINDOWS)
constexpr const char* kPlatformName = "Windows";
#elif defined(__APPLE__)
constexpr const char* kPlatformName = "macOS";
#else
constexpr const char* kPlatformName = "Linux";
#endif

}  // namespace tq
