// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// A tiny self-contained test framework (no third-party dependencies).
#pragma once

#include <functional>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace tq::test {

struct TestCase {
    std::string suite;
    std::string name;
    std::function<void()> fn;
};

std::vector<TestCase>& registry();

struct Registrar {
    Registrar(const char* suite, const char* name, std::function<void()> fn) {
        registry().push_back(TestCase{suite, name, std::move(fn)});
    }
};

// Thrown by ASSERT_* to abort the current test.
struct AssertionFailed : std::runtime_error {
    using std::runtime_error::runtime_error;
};

void recordFailure(const char* file, int line, const std::string& message);
int failureCount();

template <typename T>
std::string show(const T& v) {
    std::ostringstream os;
    if constexpr (std::is_same_v<T, bool>) {
        os << (v ? "true" : "false");
    } else if constexpr (std::is_enum_v<T>) {
        os << static_cast<long long>(v);
    } else if constexpr (requires(std::ostream& o, const T& x) { o << x; }) {
        os << v;
    } else {
        os << "<value>";
    }
    return os.str();
}

}  // namespace tq::test

#define TQ_CONCAT_INNER(a, b) a##b
#define TQ_CONCAT(a, b) TQ_CONCAT_INNER(a, b)

#define TEST(suite, name)                                                                         \
    static void TQ_CONCAT(test_##suite##_, name)();                                               \
    static ::tq::test::Registrar TQ_CONCAT(registrar_##suite##_, name)(#suite, #name,             \
                                                                      &TQ_CONCAT(test_##suite##_, name)); \
    static void TQ_CONCAT(test_##suite##_, name)()

#define TQ_CHECK(cond, fatal, text)                                              \
    do {                                                                         \
        if (!(cond)) {                                                           \
            ::tq::test::recordFailure(__FILE__, __LINE__, text);                 \
            if (fatal) throw ::tq::test::AssertionFailed(text);                  \
        }                                                                        \
    } while (0)

#define EXPECT_TRUE(c) TQ_CHECK((c), false, "expected true: " #c)
#define EXPECT_FALSE(c) TQ_CHECK(!(c), false, "expected false: " #c)
#define ASSERT_TRUE(c) TQ_CHECK((c), true, "expected true: " #c)
#define ASSERT_FALSE(c) TQ_CHECK(!(c), true, "expected false: " #c)

#define TQ_CMP(a, b, op, fatal)                                                                     \
    do {                                                                                            \
        const auto tq_a_ = (a);                                                                         \
        const auto tq_b_ = (b);                                                                         \
        if (!(tq_a_ op tq_b_)) {                                                                    \
            std::string tq_msg_ = std::string("expected ") + #a + " " #op " " + #b + " (" +       \
                                  ::tq::test::show(tq_a_) + " vs " + ::tq::test::show(tq_b_) + ")"; \
            ::tq::test::recordFailure(__FILE__, __LINE__, tq_msg_);                                 \
            if (fatal) throw ::tq::test::AssertionFailed(tq_msg_);                                  \
        }                                                                                           \
    } while (0)

#define EXPECT_EQ(a, b) TQ_CMP(a, b, ==, false)
#define EXPECT_NE(a, b) TQ_CMP(a, b, !=, false)
#define EXPECT_LT(a, b) TQ_CMP(a, b, <, false)
#define EXPECT_LE(a, b) TQ_CMP(a, b, <=, false)
#define EXPECT_GT(a, b) TQ_CMP(a, b, >, false)
#define EXPECT_GE(a, b) TQ_CMP(a, b, >=, false)
#define ASSERT_EQ(a, b) TQ_CMP(a, b, ==, true)
#define ASSERT_NE(a, b) TQ_CMP(a, b, !=, true)
#define ASSERT_GT(a, b) TQ_CMP(a, b, >, true)
#define ASSERT_GE(a, b) TQ_CMP(a, b, >=, true)

#define EXPECT_THROW(stmt, type)                                                     \
    do {                                                                             \
        bool tq_threw_ = false;                                                      \
        try {                                                                        \
            stmt;                                                                    \
        } catch (const type&) {                                                      \
            tq_threw_ = true;                                                        \
        }                                                                            \
        TQ_CHECK(tq_threw_, false, "expected " #stmt " to throw " #type);            \
    } while (0)
