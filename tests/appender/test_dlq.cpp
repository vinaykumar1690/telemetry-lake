#include "appender/dlq_producer.hpp"

#include <gtest/gtest.h>

using namespace appender;
using namespace std::chrono_literals;

TEST(DlqTest, HeadersDescribeSourceAndError) {
    const AppenderError error{ErrorKind::Poison, "invalid OTLP/JSON"};
    const DlqRecord record{.key = "k", .payload = "bytes", .source_topic = "otel-logs",
                           .source_partition = 3, .source_offset = 1234, .error = &error};
    const auto headers = dlqHeaders(record, "appender-0");
    const std::vector<std::pair<std::string, std::string>> expected{
        {"x-source-topic", "otel-logs"},  {"x-source-partition", "3"},
        {"x-source-offset", "1234"},      {"x-appender-id", "appender-0"},
        {"x-error-kind", "poison"},       {"x-error-message", "invalid OTLP/JSON"},
    };
    EXPECT_EQ(headers, expected);
}

TEST(DlqTest, UndeliverableRecordIsRetryable) {
    // Nothing listens on port 1; delivery fails once message.timeout.ms passes
    auto producer = KafkaDlqProducer::create(
        {.brokers = "127.0.0.1:1", .topic = "dlq", .appender_id = "t", .delivery_timeout = 300ms});
    ASSERT_TRUE(producer) << producer.error().message;
    const AppenderError error{ErrorKind::Poison, "bad"};
    auto result = (*producer)->send({.payload = "x", .source_topic = "t", .error = &error});
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().kind, ErrorKind::Retryable);
}
