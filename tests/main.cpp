// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <set>

#include "common/logging.h"
#include "test.h"

namespace tq::test {

namespace {
int g_failures = 0;
}

std::vector<TestCase>& registry() {
    static std::vector<TestCase> tests;
    return tests;
}

void recordFailure(const char* file, int line, const std::string& message) {
    ++g_failures;
    std::cerr << "    " << file << ":" << line << ": " << message << "\n";
}

int failureCount() { return g_failures; }

}  // namespace tq::test

int main(int argc, char** argv) {
    std::set<std::string> suites;
    std::string only;
    bool list = false;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--suite" && i + 1 < argc) suites.insert(argv[++i]);
        else if (a == "--test" && i + 1 < argc) only = argv[++i];
        else if (a == "--list") list = true;
        else if (a == "--verbose") tq::Logger::instance().setLevel(tq::LogLevel::Debug);
    }
    if (std::getenv("TQ_TEST_VERBOSE") == nullptr && only.empty()) {
        tq::Logger::instance().setLevel(tq::LogLevel::Error);
    }
    auto& tests = tq::test::registry();
    if (list) {
        for (const auto& t : tests) std::cout << t.suite << "." << t.name << "\n";
        return 0;
    }
    int run = 0, failedTests = 0;
    for (const auto& t : tests) {
        if (!suites.empty() && !suites.count(t.suite)) continue;
        if (!only.empty() && only != t.name && only != t.suite + "." + t.name) continue;
        int before = tq::test::failureCount();
        auto start = std::chrono::steady_clock::now();
        std::cout << "[ RUN      ] " << t.suite << "." << t.name << std::endl;
        try {
            t.fn();
        } catch (const tq::test::AssertionFailed&) {
        } catch (const std::exception& e) {
            tq::test::recordFailure(__FILE__, __LINE__, std::string("unexpected exception: ") + e.what());
        }
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
        ++run;
        if (tq::test::failureCount() > before) {
            ++failedTests;
            std::cout << "[  FAILED  ] " << t.suite << "." << t.name << " (" << ms << " ms)" << std::endl;
        } else {
            std::cout << "[       OK ] " << t.suite << "." << t.name << " (" << ms << " ms)" << std::endl;
        }
    }
    std::cout << run << " tests run, " << failedTests << " failed" << std::endl;
    if (run == 0) {
        std::cerr << "no tests matched" << std::endl;
        return 2;
    }
    return failedTests == 0 ? 0 : 1;
}
