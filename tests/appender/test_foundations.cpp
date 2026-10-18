#include "appender/appender_config.hpp"
#include "appender/backoff.hpp"
#include "appender/bounded_queue.hpp"
#include "appender/error.hpp"
#include "appender/sql.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <map>
#include <random>
#include <string>
#include <thread>

using namespace appender;
using namespace std::chrono_literals;

namespace {

// Sets environment variables for one test and restores them afterwards
class ScopedEnv {
public:
    void set(const std::string& name, const std::string& value) {
        remember(name);
        ::setenv(name.c_str(), value.c_str(), 1);
    }
    void unset(const std::string& name) {
        remember(name);
        ::unsetenv(name.c_str());
    }
    ~ScopedEnv() {
        for (const auto& [name, old] : saved_) {
            if (old) ::setenv(name.c_str(), old->c_str(), 1);
            else ::unsetenv(name.c_str());
        }
    }

private:
    void remember(const std::string& name) {
        if (saved_.contains(name)) return;
        const char* old = std::getenv(name.c_str());
        saved_[name] = old ? std::optional<std::string>(old) : std::nullopt;
    }
    std::map<std::string, std::optional<std::string>> saved_;
};

void setRequired(ScopedEnv& env) {
    env.set("KAFKA_BROKERS", "localhost:9092");
    env.set("ICEBERG_CATALOG_URI", "http://localhost:19120/iceberg/");
    env.set("S3_ENDPOINT", "localhost:9000");
    env.set("S3_ACCESS_KEY", "key");
    env.set("S3_SECRET_KEY", "secret");
    env.set("S3_BUCKET", "bucket");
    for (const char* name : {"KAFKA_TOPIC", "KAFKA_DLQ_TOPIC", "BUFFER_SIZE_MB", "MAX_STAGED_MB",
                             "ICEBERG_PARTITION_SPEC", "REVOKE_TIMEOUT_SECONDS",
                             "MAX_POLL_INTERVAL_MS", "ICEBERG_COMMIT_RETRIES",
                             "ICEBERG_TABLE_NAME", "ICEBERG_OFFSETS_TABLE", "ICEBERG_NAMESPACE",
                             "ICEBERG_RETRY_BASE_DELAY_MS", "ICEBERG_RETRY_MAX_DELAY_MS",
                             "S3_URL_STYLE", "APPENDER_ID"}) {
        env.unset(name);
    }
}

}  // namespace

TEST(AppenderConfigTest, Defaults) {
    ScopedEnv env;
    setRequired(env);
    const auto config = AppenderConfig::fromEnv();
    EXPECT_EQ(config.topic, "otel-logs");
    EXPECT_EQ(config.dlq_topic, "otel-logs-dlq");
    EXPECT_EQ(config.partition_spec, PartitionSpec::Day);
    EXPECT_EQ(config.flush_bytes, 100u * 1024 * 1024);
    EXPECT_GE(config.pipeline_threads, 1u);
    EXPECT_LE(config.pipeline_threads, 16u);
    EXPECT_FALSE(config.s3_use_ssl);
    EXPECT_FALSE(config.appender_id.empty());
    EXPECT_EQ(config.qualifiedLogsTable(), R"(lake."default"."logs")");
    EXPECT_EQ(config.qualifiedOffsetsTable(), R"(lake."default"."_appender_offsets")");
}

TEST(AppenderConfigTest, MissingRequiredVariableThrows) {
    ScopedEnv env;
    setRequired(env);
    env.unset("S3_BUCKET");
    EXPECT_THROW(AppenderConfig::fromEnv(), std::runtime_error);
}

TEST(AppenderConfigTest, DlqTopicFollowsTopic) {
    ScopedEnv env;
    setRequired(env);
    env.set("KAFKA_TOPIC", "logs");
    EXPECT_EQ(AppenderConfig::fromEnv().dlq_topic, "logs-dlq");
    env.set("KAFKA_DLQ_TOPIC", "poison");
    EXPECT_EQ(AppenderConfig::fromEnv().dlq_topic, "poison");
}

TEST(AppenderConfigTest, RejectsInvalidValues) {
    ScopedEnv env;
    setRequired(env);
    env.set("BUFFER_SIZE_MB", "abc");
    EXPECT_THROW(AppenderConfig::fromEnv(), std::runtime_error);
    env.set("BUFFER_SIZE_MB", "-5");
    EXPECT_THROW(AppenderConfig::fromEnv(), std::runtime_error);
    env.unset("BUFFER_SIZE_MB");

    env.set("ICEBERG_COMMIT_RETRIES", "0");
    EXPECT_THROW(AppenderConfig::fromEnv(), std::runtime_error);
    env.unset("ICEBERG_COMMIT_RETRIES");

    env.set("ICEBERG_PARTITION_SPEC", "week");
    EXPECT_THROW(AppenderConfig::fromEnv(), std::runtime_error);
}

TEST(AppenderConfigTest, CrossFieldValidation) {
    ScopedEnv env;
    setRequired(env);
    env.set("BUFFER_SIZE_MB", "200");
    env.set("MAX_STAGED_MB", "100");
    EXPECT_THROW(AppenderConfig::fromEnv(), std::runtime_error);
    env.unset("BUFFER_SIZE_MB");
    env.unset("MAX_STAGED_MB");

    env.set("REVOKE_TIMEOUT_SECONDS", "300");
    env.set("MAX_POLL_INTERVAL_MS", "300000");
    EXPECT_THROW(AppenderConfig::fromEnv(), std::runtime_error);
    env.unset("REVOKE_TIMEOUT_SECONDS");
    env.unset("MAX_POLL_INTERVAL_MS");

    env.set("ICEBERG_OFFSETS_TABLE", "logs");
    EXPECT_THROW(AppenderConfig::fromEnv(), std::runtime_error);
}

TEST(AppenderConfigTest, EndpointSchemeSetsSsl) {
    ScopedEnv env;
    setRequired(env);
    env.set("S3_ENDPOINT", "https://s3.example.com");
    auto config = AppenderConfig::fromEnv();
    EXPECT_TRUE(config.s3_use_ssl);
    EXPECT_EQ(config.s3_endpoint, "s3.example.com");

    env.set("S3_ENDPOINT", "http://minio:9000");
    config = AppenderConfig::fromEnv();
    EXPECT_FALSE(config.s3_use_ssl);
    EXPECT_EQ(config.s3_endpoint, "minio:9000");
}

TEST(SqlQuotingTest, LiteralDoublesQuotesAndKeepsBackslashes) {
    EXPECT_EQ(sql::literal("plain"), "'plain'");
    EXPECT_EQ(sql::literal("it's"), "'it''s'");
    EXPECT_EQ(sql::literal(R"(C:\path)"), R"('C:\path')");
    EXPECT_EQ(sql::literal(""), "''");
}

TEST(SqlQuotingTest, IdentifierDoublesQuotes) {
    EXPECT_EQ(sql::identifier("logs"), "\"logs\"");
    EXPECT_EQ(sql::identifier("we\"ird"), "\"we\"\"ird\"");
}

TEST(BackoffTest, CeilingDoublesAndCaps) {
    EXPECT_EQ(backoffCeiling(0, 100ms, 5000ms), 100ms);
    EXPECT_EQ(backoffCeiling(1, 100ms, 5000ms), 200ms);
    EXPECT_EQ(backoffCeiling(5, 100ms, 5000ms), 3200ms);
    EXPECT_EQ(backoffCeiling(6, 100ms, 5000ms), 5000ms);
}

TEST(BackoffTest, NoOverflowForLargeAttempts) {
    EXPECT_EQ(backoffCeiling(64, 100ms, 5000ms), 5000ms);
    EXPECT_EQ(backoffCeiling(10'000, 100ms, 5000ms), 5000ms);
}

TEST(BackoffTest, JitterStaysWithinBounds) {
    std::mt19937 rng(42);
    for (int attempt = 0; attempt < 100; ++attempt) {
        const auto delay = backoffWithJitter(attempt, 100ms, 5000ms, rng);
        const auto ceiling = backoffCeiling(attempt, 100ms, 5000ms);
        EXPECT_GE(delay, ceiling / 2);
        EXPECT_LE(delay, ceiling);
    }
}

TEST(BoundedQueueTest, RespectsWeightCapacity) {
    auto weigh = [](const std::string& s) { return s.size(); };
    BoundedQueue<std::string, decltype(weigh)> queue(10, weigh);
    std::string a = "12345", b = "123456", c = "12345";
    EXPECT_TRUE(queue.try_push(a));
    EXPECT_TRUE(a.empty());  // moved from
    EXPECT_FALSE(queue.try_push(b));
    EXPECT_EQ(b, "123456");  // untouched on failure
    EXPECT_TRUE(queue.try_push(c));
    EXPECT_EQ(queue.weight(), 10u);
    EXPECT_EQ(queue.pop_wait(0ms), "12345");
    EXPECT_EQ(queue.weight(), 5u);
}

TEST(BoundedQueueTest, OversizedItemAcceptedWhenEmpty) {
    auto weigh = [](const std::string& s) { return s.size(); };
    BoundedQueue<std::string, decltype(weigh)> queue(4, weigh);
    std::string big = "much too large";
    EXPECT_TRUE(queue.try_push(big));
    std::string small = "x";
    EXPECT_FALSE(queue.try_push(small));
}

TEST(BoundedQueueTest, PopTimesOutWhenEmpty) {
    BoundedQueue<int> queue(4);
    EXPECT_FALSE(queue.pop_wait(10ms).has_value());
}

TEST(BoundedQueueTest, PushWaitBlocksUntilRoom) {
    BoundedQueue<int> queue(1);
    ASSERT_TRUE(queue.push_wait(1));
    std::atomic<bool> pushed = false;
    std::thread producer([&] {
        queue.push_wait(2);
        pushed = true;
    });
    std::this_thread::sleep_for(50ms);
    EXPECT_FALSE(pushed);
    EXPECT_EQ(queue.pop_wait(100ms), 1);
    producer.join();
    EXPECT_TRUE(pushed);
    EXPECT_EQ(queue.pop_wait(100ms), 2);
}

TEST(BoundedQueueTest, CloseWakesWaitersAndDrains) {
    BoundedQueue<int> queue(1);
    ASSERT_TRUE(queue.push_wait(1));
    std::thread producer([&] { EXPECT_FALSE(queue.push_wait(2)); });
    std::this_thread::sleep_for(20ms);
    queue.close();
    producer.join();
    int extra = 3;
    EXPECT_FALSE(queue.try_push(extra));
    EXPECT_EQ(queue.pop_wait(10ms), 1);  // still drains
    EXPECT_FALSE(queue.pop_wait(10ms).has_value());
}

TEST(BoundedQueueTest, RemoveIfReleasesCapacity) {
    BoundedQueue<int> queue(3);
    for (int i : {1, 2, 3}) ASSERT_TRUE(queue.push_wait(i));
    EXPECT_EQ(queue.remove_if([](int v) { return v % 2 == 1; }), 2u);
    EXPECT_EQ(queue.size(), 1u);
    EXPECT_EQ(queue.pop_wait(0ms), 2);
}

TEST(ErrorTest, HelpersCarryKind) {
    Result<int> r = poison("bad payload");
    ASSERT_FALSE(r);
    EXPECT_EQ(r.error().kind, ErrorKind::Poison);
    EXPECT_EQ(r.error().message, "bad payload");
    EXPECT_EQ(toString(ErrorKind::Indeterminate), "indeterminate");
}
