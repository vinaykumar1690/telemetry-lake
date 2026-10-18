#include "appender/health.hpp"

#include <gtest/gtest.h>

using namespace appender;
using namespace std::chrono_literals;

namespace {

HealthSnapshot healthy() {
    HealthSnapshot s;
    s.now = std::chrono::steady_clock::now();
    s.polling = true;
    s.last_poll = s.now - 1s;
    return s;
}

}  // namespace

TEST(HealthTest, ReadyWhenPollingAndCommitting) {
    EXPECT_TRUE(evaluateReadiness(healthy()).ready);
}

TEST(HealthTest, NotReadyCases) {
    auto s = healthy();
    s.fatal = true;
    EXPECT_FALSE(evaluateReadiness(s).ready);

    s = healthy();
    s.polling = false;
    EXPECT_FALSE(evaluateReadiness(s).ready);

    s = healthy();
    s.last_poll = s.now - 60s;
    EXPECT_EQ(evaluateReadiness(s).reason, "Kafka poll loop is stalled");

    s = healthy();
    s.consecutive_commit_failures = 3;
    EXPECT_EQ(evaluateReadiness(s).reason, "3 consecutive Iceberg commit failures");
}

TEST(HealthTest, MetricsUsePrometheusFormat) {
    auto s = healthy();
    s.commits = 12;
    s.staged_bytes = 2048;
    s.last_commit_success = s.now - 5s;
    const auto text = formatMetrics(s);
    EXPECT_NE(text.find("# TYPE appender_commits_total counter\nappender_commits_total 12\n"),
              std::string::npos);
    EXPECT_NE(text.find("appender_staged_bytes 2048\n"), std::string::npos);
    EXPECT_NE(text.find("appender_seconds_since_last_commit 5"), std::string::npos);
    EXPECT_NE(text.find("appender_fatal 0\n"), std::string::npos);
}
