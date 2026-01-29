#include <gtest/gtest.h>
#include "../src/appender/iceberg_utils.hpp"
#include <chrono>
#include <map>
#include <string>

// Test SQL string escaping
TEST(IcebergUtilsTest, EscapeSqlString_NoSpecialChars) {
    std::string input = "hello world";
    std::string result = IcebergUtils::escapeSqlString(input);
    EXPECT_EQ(result, "hello world");
}

TEST(IcebergUtilsTest, EscapeSqlString_SingleQuotes) {
    std::string input = "it's a test";
    std::string result = IcebergUtils::escapeSqlString(input);
    EXPECT_EQ(result, "it''s a test");
}

TEST(IcebergUtilsTest, EscapeSqlString_Backslashes) {
    std::string input = "path\\to\\file";
    std::string result = IcebergUtils::escapeSqlString(input);
    EXPECT_EQ(result, "path\\\\to\\\\file");
}

TEST(IcebergUtilsTest, EscapeSqlString_MixedSpecialChars) {
    std::string input = "it's a 'path\\test'";
    std::string result = IcebergUtils::escapeSqlString(input);
    EXPECT_EQ(result, "it''s a ''path\\\\test''");
}

TEST(IcebergUtilsTest, EscapeSqlString_EmptyString) {
    std::string input = "";
    std::string result = IcebergUtils::escapeSqlString(input);
    EXPECT_EQ(result, "");
}

// Test timestamp formatting
TEST(IcebergUtilsTest, FormatTimestamp_BasicFormat) {
    // Create a known timestamp: 2024-01-15 10:30:45.123
    std::tm tm = {};
    tm.tm_year = 124;  // years since 1900
    tm.tm_mon = 0;     // January (0-based)
    tm.tm_mday = 15;
    tm.tm_hour = 10;
    tm.tm_min = 30;
    tm.tm_sec = 45;

    auto time_point = std::chrono::system_clock::from_time_t(timegm(&tm));
    // Add 123 milliseconds
    time_point += std::chrono::milliseconds(123);

    std::string result = IcebergUtils::formatTimestamp(time_point);
    EXPECT_EQ(result, "2024-01-15 10:30:45.123");
}

TEST(IcebergUtilsTest, FormatTimestamp_ZeroMilliseconds) {
    std::tm tm = {};
    tm.tm_year = 124;
    tm.tm_mon = 5;  // June
    tm.tm_mday = 1;
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;

    auto time_point = std::chrono::system_clock::from_time_t(timegm(&tm));

    std::string result = IcebergUtils::formatTimestamp(time_point);
    EXPECT_EQ(result, "2024-06-01 00:00:00.000");
}

// Test attributes map formatting
TEST(IcebergUtilsTest, FormatAttributesMap_EmptyMap) {
    std::map<std::string, std::string> attrs;
    std::string result = IcebergUtils::formatAttributesMap(attrs);
    EXPECT_EQ(result, "MAP([], [])");
}

TEST(IcebergUtilsTest, FormatAttributesMap_SingleEntry) {
    std::map<std::string, std::string> attrs = {{"key1", "value1"}};
    std::string result = IcebergUtils::formatAttributesMap(attrs);
    EXPECT_EQ(result, "MAP(['key1'], ['value1'])");
}

TEST(IcebergUtilsTest, FormatAttributesMap_MultipleEntries) {
    std::map<std::string, std::string> attrs = {
        {"key1", "value1"},
        {"key2", "value2"}
    };
    std::string result = IcebergUtils::formatAttributesMap(attrs);
    // std::map is ordered, so key1 comes before key2
    EXPECT_EQ(result, "MAP(['key1', 'key2'], ['value1', 'value2'])");
}

TEST(IcebergUtilsTest, FormatAttributesMap_SpecialCharsInValues) {
    std::map<std::string, std::string> attrs = {
        {"message", "it's a test"},
        {"path", "c:\\temp"}
    };
    std::string result = IcebergUtils::formatAttributesMap(attrs);
    // Should escape single quotes and backslashes
    EXPECT_EQ(result, "MAP(['message', 'path'], ['it''s a test', 'c:\\\\temp'])");
}

// Test record size estimation
TEST(IcebergUtilsTest, EstimateRecordsSize_EmptyVector) {
    std::vector<TransformedLogRecord> records;
    size_t result = IcebergUtils::estimateRecordsSize(records);
    EXPECT_EQ(result, 0u);
}

TEST(IcebergUtilsTest, EstimateRecordsSize_SingleRecord) {
    TransformedLogRecord record;
    record.kafka_topic = "test-topic";
    record.kafka_partition = 0;
    record.kafka_offset = 100;
    record.body = "test message body";
    record.severity = "INFO";
    record.service_name = "test-service";
    record.deployment_environment = "production";
    record.host_name = "host1";
    record.trace_id = "abc123";
    record.span_id = "def456";
    record.attributes = {{"key1", "value1"}};

    std::vector<TransformedLogRecord> records = {record};
    size_t result = IcebergUtils::estimateRecordsSize(records);

    // Calculate expected size
    size_t expected = record.kafka_topic.size() + sizeof(record.kafka_partition) +
                      sizeof(record.kafka_offset) + record.body.size() + record.severity.size() +
                      record.service_name.size() + record.deployment_environment.size() +
                      record.host_name.size() + record.trace_id.size() + record.span_id.size() +
                      4 + 6 + // "key1" + "value1"
                      100;    // overhead

    EXPECT_EQ(result, expected);
}

TEST(IcebergUtilsTest, EstimateRecordsSize_MultipleRecords) {
    TransformedLogRecord record1;
    record1.kafka_topic = "topic1";
    record1.body = "body1";
    record1.severity = "INFO";

    TransformedLogRecord record2;
    record2.kafka_topic = "topic2";
    record2.body = "body2";
    record2.severity = "ERROR";

    std::vector<TransformedLogRecord> records = {record1, record2};
    size_t result = IcebergUtils::estimateRecordsSize(records);

    // Should include overhead for each record
    EXPECT_GE(result, 200u); // At least 2 * 100 overhead
}

// Test getFullTableName
TEST(IcebergUtilsTest, GetFullTableName) {
    std::string result = IcebergUtils::getFullTableName("logs");
    EXPECT_EQ(result, "iceberg_catalog.default.logs");
}

TEST(IcebergUtilsTest, GetFullTableName_CustomTable) {
    std::string result = IcebergUtils::getFullTableName("otel_logs_v2");
    EXPECT_EQ(result, "iceberg_catalog.default.otel_logs_v2");
}

// Test buildInsertSQL
TEST(IcebergUtilsTest, BuildInsertSQL_SingleRecord) {
    TransformedLogRecord record;
    record.kafka_topic = "test-topic";
    record.kafka_partition = 0;
    record.kafka_offset = 100;
    record.timestamp = std::chrono::system_clock::now();
    record.body = "test body";
    record.severity = "INFO";
    record.service_name = "service1";
    record.deployment_environment = "prod";
    record.host_name = "host1";
    record.trace_id = "trace123";
    record.span_id = "span456";
    record.attributes = {};

    std::vector<TransformedLogRecord> records = {record};
    std::string sql = IcebergUtils::buildInsertSQL(records, "local_buffer_0");

    // Verify SQL structure
    EXPECT_TRUE(sql.find("INSERT INTO local_buffer_0 VALUES") != std::string::npos);
    EXPECT_TRUE(sql.find("'test-topic'") != std::string::npos);
    EXPECT_TRUE(sql.find("'test body'") != std::string::npos);
    EXPECT_TRUE(sql.find("'INFO'") != std::string::npos);
    EXPECT_TRUE(sql.find("MAP([], [])") != std::string::npos);
    EXPECT_TRUE(sql.back() == ';');
}

TEST(IcebergUtilsTest, BuildInsertSQL_EscapesSpecialChars) {
    TransformedLogRecord record;
    record.kafka_topic = "test-topic";
    record.kafka_partition = 0;
    record.kafka_offset = 100;
    record.timestamp = std::chrono::system_clock::now();
    record.body = "it's a test with 'quotes'";
    record.severity = "INFO";
    record.service_name = "service1";
    record.deployment_environment = "prod";
    record.host_name = "host1";
    record.trace_id = "";
    record.span_id = "";
    record.attributes = {};

    std::vector<TransformedLogRecord> records = {record};
    std::string sql = IcebergUtils::buildInsertSQL(records, "buffer");

    // Verify escaping
    EXPECT_TRUE(sql.find("it''s a test with ''quotes''") != std::string::npos);
}

TEST(IcebergUtilsTest, BuildInsertSQL_MultipleRecords) {
    TransformedLogRecord record1;
    record1.kafka_topic = "topic";
    record1.kafka_partition = 0;
    record1.kafka_offset = 1;
    record1.timestamp = std::chrono::system_clock::now();
    record1.body = "body1";
    record1.severity = "INFO";

    TransformedLogRecord record2;
    record2.kafka_topic = "topic";
    record2.kafka_partition = 0;
    record2.kafka_offset = 2;
    record2.timestamp = std::chrono::system_clock::now();
    record2.body = "body2";
    record2.severity = "ERROR";

    std::vector<TransformedLogRecord> records = {record1, record2};
    std::string sql = IcebergUtils::buildInsertSQL(records, "buffer");

    // Should have comma-separated values
    EXPECT_TRUE(sql.find("), (") != std::string::npos);
    EXPECT_TRUE(sql.find("'body1'") != std::string::npos);
    EXPECT_TRUE(sql.find("'body2'") != std::string::npos);
}
