#include "appender/duckdb_session.hpp"
#include "appender/schema.hpp"
#include "appender/staging_store.hpp"
#include "appender/utf8.hpp"

#include <gtest/gtest.h>

#include <format>
#include <thread>

using namespace appender;
using namespace std::chrono_literals;

namespace {

const TableNames kTables{"logs", "offsets"};

LogRow makeRow(std::int64_t offset, std::string body = "hello") {
    auto resource = std::make_shared<ResourceFields>();
    resource->service_name = "svc";
    resource->attributes = {{"service.name", "svc"}, {"k8s.pod", "p-1"}};
    auto scope = std::make_shared<ScopeFields>(ScopeFields{"scope", "1.0"});

    LogRow row;
    row.kafka_partition = 2;
    row.kafka_offset = offset;
    row.timestamp_us = 1'700'000'000'123'456LL;
    row.time_unix_nano = 1'700'000'000'123'456'789ULL;
    row.severity_number = 9;
    row.severity_text = "INFO";
    row.body = std::move(body);
    row.trace_id = "0102";
    row.flags = 1;
    row.resource = resource;
    row.scope = scope;
    row.attributes = {{"a", "1"}};
    return row;
}

class StagingTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto session = DuckdbSession::openLocal(kTables);
        ASSERT_TRUE(session) << session.error().message;
        session_ = std::move(*session);
        conn_ = session_->connect();
    }

    duckdb::Value scalar(const std::string& sql) {
        auto result = query(*conn_, sql);
        EXPECT_TRUE(result) << (result ? "" : result.error().message);
        return (*result)->GetValue(0, 0);
    }

    std::unique_ptr<DuckdbSession> session_;
    std::unique_ptr<duckdb::Connection> conn_;
};

}  // namespace

TEST(Utf8Test, ValidatesAndSanitizes) {
    EXPECT_TRUE(utf8::isValid("plain ascii"));
    EXPECT_TRUE(utf8::isValid("caf\xC3\xA9 \xF0\x9F\x98\x80"));
    EXPECT_FALSE(utf8::isValid("bad \xFF byte"));
    EXPECT_FALSE(utf8::isValid("\xC0\xAF"));          // overlong
    EXPECT_FALSE(utf8::isValid("\xED\xA0\x80"));      // surrogate
    EXPECT_FALSE(utf8::isValid("truncated \xE2\x82"));
    EXPECT_EQ(utf8::sanitize("a\xFF" "b"), "a\xEF\xBF\xBD" "b");
    EXPECT_EQ(utf8::sanitize("caf\xC3\xA9"), "caf\xC3\xA9");
}

TEST_F(StagingTest, OpenLocalCreatesTablesWithSchema) {
    EXPECT_EQ(scalar("SELECT count(*) FROM logs").GetValue<int64_t>(), 0);
    EXPECT_EQ(scalar("SELECT count(*) FROM offsets").GetValue<int64_t>(), 0);
}

TEST_F(StagingTest, EnsureTableRejectsDifferentSchema) {
    ASSERT_TRUE(query(*conn_, "CREATE TABLE other (a INTEGER)"));
    auto result = ensureTable(*conn_, "other", schema::kOffsetsColumns);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().kind, ErrorKind::Fatal);
    // Same columns are accepted on the second call
    EXPECT_TRUE(ensureTable(*conn_, "offsets", schema::kOffsetsColumns));
}

TEST_F(StagingTest, AppendSealRoundTripsValues) {
    StagingRegistry registry(session_->database(), 1 << 20);
    ASSERT_TRUE(registry.add(2));
    auto staging = registry.find(2);
    ASSERT_TRUE(staging);

    const std::string tricky = "back\\slash 'quote' \"dq\" emoji \xF0\x9F\x98\x80 nul\0x";
    std::vector<LogRow> rows{makeRow(10, std::string(tricky.data(), tricky.size())),
                             makeRow(11, "bad \xFF utf8")};
    rows[1].attributes = {{"dup", "first"}, {"dup", "second"}};
    rows[1].trace_id.clear();
    rows[1].timestamp_us.reset();
    ASSERT_TRUE(staging->append(rows, 11, 500));
    EXPECT_EQ(registry.budget().used(), 500u);

    auto sealed = registry.sealAll();
    ASSERT_TRUE(sealed) << sealed.error().message;
    ASSERT_EQ(sealed->size(), 1u);
    const SealedBuffer& buffer = (*sealed)[0];
    EXPECT_EQ(buffer.partition, 2);
    EXPECT_EQ(buffer.max_offset, 11);
    EXPECT_EQ(buffer.rows, 2u);
    EXPECT_EQ(buffer.bytes, 500u);

    const auto table = std::format("\"{}\"", buffer.table);
    EXPECT_EQ(scalar(std::format("SELECT body FROM {} WHERE _kafka_offset = 10", table)).ToString(),
              std::string(tricky.data(), tricky.size()));
    EXPECT_EQ(scalar(std::format("SELECT body FROM {} WHERE _kafka_offset = 11", table)).ToString(),
              "bad \xEF\xBF\xBD utf8");
    EXPECT_EQ(scalar(std::format("SELECT log_attributes['dup'] FROM {} WHERE _kafka_offset = 11",
                                 table)).ToString(), "second");
    EXPECT_TRUE(scalar(std::format("SELECT trace_id FROM {} WHERE _kafka_offset = 11", table)).IsNull());
    EXPECT_TRUE(scalar(std::format("SELECT \"timestamp\" FROM {} WHERE _kafka_offset = 11", table)).IsNull());
    EXPECT_EQ(scalar(std::format("SELECT epoch_us(\"timestamp\") FROM {} WHERE _kafka_offset = 10",
                                 table)).GetValue<int64_t>(), 1'700'000'000'123'456LL);
    EXPECT_EQ(scalar(std::format("SELECT resource_attributes['k8s.pod'] FROM {} LIMIT 1", table))
                  .ToString(), "p-1");
    EXPECT_EQ(scalar(std::format("SELECT scope_name FROM {} LIMIT 1", table)).ToString(), "scope");

    // Sealed bytes stay accounted until the committer drops the table
    EXPECT_EQ(registry.budget().used(), 500u);
    registry.dropSealed(buffer);
    EXPECT_EQ(registry.budget().used(), 0u);
}

TEST_F(StagingTest, SealSwitchesGenerationAndSkipsCleanPartitions) {
    StagingRegistry registry(session_->database(), 1 << 20);
    ASSERT_TRUE(registry.add(2));
    ASSERT_TRUE(registry.add(3));
    auto staging = registry.find(2);
    std::vector<LogRow> rows{makeRow(1)};
    ASSERT_TRUE(staging->append(rows, 1, 10));

    auto first = registry.sealAll();
    ASSERT_TRUE(first);
    ASSERT_EQ(first->size(), 1u);  // partition 3 had nothing

    ASSERT_TRUE(staging->append(rows, 2, 10));
    auto second = registry.sealAll();
    ASSERT_TRUE(second);
    ASSERT_EQ(second->size(), 1u);
    EXPECT_NE((*first)[0].table, (*second)[0].table);
    EXPECT_EQ((*second)[0].max_offset, 2);

    auto none = registry.sealAll();
    ASSERT_TRUE(none);
    EXPECT_TRUE(none->empty());
}

TEST_F(StagingTest, OffsetOnlyAdvanceIsSealed) {
    StagingRegistry registry(session_->database(), 1 << 20);
    ASSERT_TRUE(registry.add(0));
    registry.find(0)->advanceOffset(42);  // e.g. a poison message sent to the DLQ
    auto sealed = registry.sealAll();
    ASSERT_TRUE(sealed);
    ASSERT_EQ(sealed->size(), 1u);
    EXPECT_EQ((*sealed)[0].rows, 0u);
    EXPECT_EQ((*sealed)[0].max_offset, 42);
}

TEST_F(StagingTest, SealExcludesPartitions) {
    StagingRegistry registry(session_->database(), 1 << 20);
    ASSERT_TRUE(registry.add(0));
    ASSERT_TRUE(registry.add(1));
    registry.find(0)->advanceOffset(1);
    registry.find(1)->advanceOffset(1);
    auto sealed = registry.sealAll({1});
    ASSERT_TRUE(sealed);
    ASSERT_EQ(sealed->size(), 1u);
    EXPECT_EQ((*sealed)[0].partition, 0);
}

TEST_F(StagingTest, DiscardAndRemoveReleaseBudget) {
    StagingRegistry registry(session_->database(), 1 << 20);
    ASSERT_TRUE(registry.add(0));
    std::vector<LogRow> rows{makeRow(1)};
    ASSERT_TRUE(registry.find(0)->append(rows, 1, 100));
    EXPECT_TRUE(registry.oldestUnsealedRow().has_value());
    registry.find(0)->discard();
    EXPECT_EQ(registry.budget().used(), 0u);
    EXPECT_FALSE(registry.oldestUnsealedRow().has_value());
    auto sealed = registry.sealAll();
    ASSERT_TRUE(sealed);
    EXPECT_TRUE(sealed->empty());

    ASSERT_TRUE(registry.find(0)->append(rows, 2, 100));
    registry.remove(0);
    EXPECT_EQ(registry.budget().used(), 0u);
    EXPECT_FALSE(registry.find(0));
}

TEST(ByteBudgetTest, WaitBelowCap) {
    ByteBudget budget(100);
    budget.add(150);
    EXPECT_FALSE(budget.waitBelowCap(10ms));
    std::thread releaser([&] {
        std::this_thread::sleep_for(20ms);
        budget.release(100);
    });
    EXPECT_TRUE(budget.waitBelowCap(1s));
    releaser.join();
    budget.release(1000);  // never underflows
    EXPECT_EQ(budget.used(), 0u);
}

TEST(DuckdbErrorTest, Classification) {
    EXPECT_EQ(classifyDuckdbError("Binder Error: column x not found"), ErrorKind::Fatal);
    EXPECT_EQ(classifyDuckdbError("Catalog Error: Table does not exist"), ErrorKind::Fatal);
    EXPECT_EQ(classifyDuckdbError("HTTP Error: 503 Service Unavailable"), ErrorKind::Retryable);
    EXPECT_EQ(classifyDuckdbError("IO Error: Connection timed out"), ErrorKind::Retryable);
}
