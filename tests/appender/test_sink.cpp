#include "appender/duckdb_iceberg.hpp"
#include "appender/staging_store.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <format>

using namespace appender;

namespace {

const TableNames kTables{"logs", "offsets"};
const SinkIdentity kIdentity{"otel-logs", "group-a", "appender-1"};

class SinkTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto session = DuckdbSession::openLocal(kTables);
        ASSERT_TRUE(session) << session.error().message;
        session_ = std::move(*session);
        conn_ = session_->connect();
        registry_ = std::make_unique<StagingRegistry>(session_->database(), 1 << 20);
    }

    SealedBuffer stage(std::int32_t partition, std::int64_t first_offset, int count) {
        if (!registry_->find(partition)) EXPECT_TRUE(registry_->add(partition));
        std::vector<LogRow> rows;
        for (int i = 0; i < count; ++i) {
            LogRow row;
            row.kafka_partition = partition;
            row.kafka_offset = first_offset + i;
            row.record_index = i;
            row.body = std::format("p{} o{}", partition, first_offset + i);
            rows.push_back(std::move(row));
        }
        EXPECT_TRUE(registry_->find(partition)->append(rows, first_offset + count - 1, 10));
        auto sealed = registry_->sealAll({});
        EXPECT_TRUE(sealed && sealed->size() == 1);
        return sealed->front();
    }

    std::int64_t count(const std::string& sql) {
        auto result = query(*conn_, sql);
        EXPECT_TRUE(result) << (result ? "" : result.error().message);
        return (*result)->GetValue(0, 0).GetValue<std::int64_t>();
    }

    std::unique_ptr<DuckdbSession> session_;
    std::unique_ptr<duckdb::Connection> conn_;
    std::unique_ptr<StagingRegistry> registry_;
};

}  // namespace

TEST_F(SinkTest, CommitWritesRowsAndOffsetsTogether) {
    DuckdbIcebergSink sink(*session_, kTables, kIdentity);
    std::vector<SealedBuffer> buffers{stage(0, 100, 3), stage(1, 7, 2)};
    const auto now = std::chrono::system_clock::now();
    ASSERT_TRUE(sink.commit(buffers, {now}));

    EXPECT_EQ(count("SELECT count(*) FROM logs"), 5);
    EXPECT_EQ(count("SELECT count(*) FROM logs WHERE _kafka_topic = 'otel-logs'"), 5);
    EXPECT_EQ(count("SELECT count(DISTINCT _ingested_at) FROM logs"), 1);
    EXPECT_EQ(count("SELECT count(*) FROM logs WHERE body = 'p1 o8'"), 1);
    EXPECT_EQ(count("SELECT _record_index FROM logs WHERE body = 'p1 o8'"), 1);
    EXPECT_EQ(count("SELECT count(*) FROM offsets"), 2);
    EXPECT_EQ(count("SELECT last_offset FROM offsets WHERE \"partition\" = 0"), 102);
    EXPECT_EQ(count("SELECT count(*) FROM offsets WHERE consumer_group = 'group-a' "
                    "AND appender_id = 'appender-1'"), 2);
}

TEST_F(SinkTest, OffsetOnlyBufferWritesNoLogRows) {
    DuckdbIcebergSink sink(*session_, kTables, kIdentity);
    ASSERT_TRUE(registry_->add(4));
    registry_->find(4)->advanceOffset(55);
    auto sealed = registry_->sealAll({});
    ASSERT_TRUE(sealed);
    ASSERT_TRUE(sink.commit(*sealed, {std::chrono::system_clock::now()}));
    EXPECT_EQ(count("SELECT count(*) FROM logs"), 0);
    EXPECT_EQ(count("SELECT last_offset FROM offsets WHERE \"partition\" = 4"), 55);
}

TEST_F(SinkTest, FailedCommitLeavesBothTablesUnchanged) {
    // The offsets insert fails, so the logs insert in the same transaction must not land
    DuckdbIcebergSink sink(*session_, {"logs", "missing_offsets"}, kIdentity);
    std::vector<SealedBuffer> buffers{stage(0, 1, 3)};
    auto result = sink.commit(buffers, {std::chrono::system_clock::now()});
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().kind, ErrorKind::Fatal);
    EXPECT_EQ(count("SELECT count(*) FROM logs"), 0);

    // The connection is usable again after the rollback
    DuckdbIcebergSink good(*session_, kTables, kIdentity);
    EXPECT_TRUE(good.commit(buffers, {std::chrono::system_clock::now()}));
    EXPECT_EQ(count("SELECT count(*) FROM logs"), 3);
}

TEST_F(SinkTest, CommitSqlShape) {
    std::vector<SealedBuffer> buffers{{.partition = 0, .table = "stg_p0_g0", .max_offset = 9, .rows = 2},
                                      {.partition = 1, .table = "stg_p1_g0", .max_offset = 3, .rows = 0}};
    const auto sql = buildCommitSql(buffers, kTables, schema::TableSchema{}, kIdentity,
                                    {std::chrono::sys_days{std::chrono::year{2026} / 10 / 18}});
    EXPECT_NE(sql.find(R"(FROM "stg_p0_g0")"), std::string::npos);
    EXPECT_EQ(sql.find("stg_p1_g0"), std::string::npos);  // no rows, offsets only
    EXPECT_NE(sql.find("TIMESTAMPTZ '2026-10-18 00:00:00.000000+00'"), std::string::npos);
    EXPECT_NE(sql.find("('group-a', 'otel-logs', 1, 3,"), std::string::npos);
}

TEST_F(SinkTest, LastCommittedReturnsMaxPerPartitionForTopic) {
    DuckdbIcebergSink sink(*session_, kTables, kIdentity);
    ASSERT_TRUE(sink.commit(std::vector{stage(0, 1, 2)}, {std::chrono::system_clock::now()}));
    ASSERT_TRUE(sink.commit(std::vector{stage(0, 3, 2)}, {std::chrono::system_clock::now()}));
    DuckdbIcebergSink other_topic(*session_, kTables, {"other", "g", "a"});
    ASSERT_TRUE(other_topic.commit(std::vector{stage(1, 500, 1)}, {std::chrono::system_clock::now()}));

    DuckdbOffsetStore store(*session_, "offsets", "otel-logs");
    const std::int32_t partitions[] = {0, 1, 2};
    auto committed = store.lastCommitted(partitions);
    ASSERT_TRUE(committed) << committed.error().message;
    EXPECT_EQ(committed->size(), 1u);  // partition 1 was another topic, 2 never committed
    EXPECT_EQ(committed->at(0), 4);
}

TEST_F(SinkTest, PruneIsSkippedWithoutRewriteSupport) {
    DuckdbOffsetStore store(*session_, "offsets", "otel-logs");
    EXPECT_TRUE(store.prune());
}

// Against a live REST catalog + S3, configured with the appender's env vars.
TEST(PromotedSinkTest, CommitWritesPromotedColumnsAndNullsFormerOnes) {
    auto promoted = std::make_shared<schema::TableSchema>(std::vector<std::string>{"k8s.pod.name"});
    auto session = DuckdbSession::openLocal(kTables, promoted);
    ASSERT_TRUE(session) << session.error().message;
    auto conn = (*session)->connect();
    ASSERT_TRUE(query(*conn, "ALTER TABLE logs ADD COLUMN formerly_promoted VARCHAR"));

    StagingRegistry registry((*session)->database(), 1 << 20, promoted);
    ASSERT_TRUE(registry.add(0));
    LogRow row;
    row.kafka_offset = 3;
    row.promoted = {"pod-9"};
    std::vector<LogRow> rows{row};
    ASSERT_TRUE(registry.find(0)->append(rows, 3, 1));
    auto sealed = registry.sealAll();
    ASSERT_TRUE(sealed);

    DuckdbIcebergSink sink(**session, kTables, kIdentity);
    ASSERT_TRUE(sink.commit(*sealed, {std::chrono::system_clock::now()}));
    auto result = query(*conn, "SELECT k8s_pod_name, formerly_promoted FROM logs");
    ASSERT_TRUE(result);
    EXPECT_EQ((*result)->GetValue(0, 0).ToString(), "pod-9");
    EXPECT_TRUE((*result)->GetValue(1, 0).IsNull());
}

// Opt-in: ICEBERG_IT=1. Uses a fresh namespace per run.
TEST(IcebergIntegrationTest, CommitAndRecoverAgainstLiveCatalog) {
    const char* enabled = std::getenv("ICEBERG_IT");
    if (!enabled || std::string_view(enabled) != "1") {
        GTEST_SKIP() << "set ICEBERG_IT=1 to run against a live catalog";
    }
    ::setenv("PROMOTED_ATTRIBUTES", "k8s.pod.name", 1);
    ::setenv("ICEBERG_NAMESPACE",
             std::format("it_{}", std::chrono::system_clock::now().time_since_epoch().count()).c_str(), 1);
    const auto config = AppenderConfig::fromEnv();
    auto session = DuckdbSession::openIceberg(config);
    ASSERT_TRUE(session) << session.error().message;

    StagingRegistry registry((*session)->database(), 1 << 20, (*session)->schema());
    ASSERT_TRUE(registry.add(0));
    // 03:00Z and 22:00Z on one UTC day are on another local day in most
    // time zones, so a partition computed in local time is caught below
    LogRow row;
    row.kafka_offset = 40;
    row.timestamp_us = 1'759'978'800'000'000LL;  // 2025-10-09T03:00:00Z
    row.attributes = {{"k", "v\\w"}};
    row.promoted = {"pod-it"};
    LogRow late = row;
    late.kafka_offset = 41;
    late.timestamp_us = 1'760'047'200'000'000LL;  // 2025-10-09T22:00:00Z
    std::vector<LogRow> rows{row, late};
    ASSERT_TRUE(registry.find(0)->append(rows, 41, 1));
    auto sealed = registry.sealAll({});
    ASSERT_TRUE(sealed);

    DuckdbIcebergSink sink(**session, (*session)->tables(), {config.topic, config.consumer_group, "it"});
    auto committed = sink.commit(*sealed, {std::chrono::system_clock::now()});
    ASSERT_TRUE(committed) << committed.error().message;

    DuckdbOffsetStore store(**session, (*session)->tables().offsets, config.topic);
    const std::int32_t partitions[] = {0};
    auto last = store.lastCommitted(partitions);
    ASSERT_TRUE(last) << last.error().message;
    EXPECT_EQ(last->at(0), 41);

    // The row's UTC day partition must survive pruning: written with a local
    // time zone's day, a reader filtering on the UTC day would skip the file
    auto conn = (*session)->connect();
    auto pruned = query(*conn, std::format("SELECT count(*) FROM {} WHERE \"timestamp\" >= TIMESTAMPTZ "
                                           "'2025-10-09 00:00:00+00' AND \"timestamp\" < TIMESTAMPTZ "
                                           "'2025-10-10 00:00:00+00'",
                                           (*session)->tables().logs));
    ASSERT_TRUE(pruned) << pruned.error().message;
    EXPECT_EQ((*pruned)->GetValue(0, 0).GetValue<int64_t>(), 2);

    auto pods = query(*conn, std::format("SELECT count(*) FROM {} WHERE k8s_pod_name = 'pod-it'",
                                         (*session)->tables().logs));
    ASSERT_TRUE(pods) << pods.error().message;
    EXPECT_EQ((*pods)->GetValue(0, 0).GetValue<int64_t>(), 2);
}

// Table metadata as the Nessie catalog returns it (trimmed), after an append
// of 1500 rows into 2 data files
constexpr std::string_view kMetadata = R"({
  "format-version": 2,
  "current-snapshot-id": 2085327308212356669,
  "snapshots": [{
    "sequence-number": 2,
    "snapshot-id": 2085327308212356669,
    "summary": {"operation": "append", "total-records": "3000", "total-data-files": "5",
                "added-data-files": "2", "added-records": "1500"}
  }]
})";

TEST(CommitFilesTest, ReadsTheCurrentAppendSnapshot) {
    auto files = commitFilesFromMetadata(kMetadata, 1500);
    ASSERT_TRUE(files);
    EXPECT_EQ(files->added_data_files, 2u);
    EXPECT_EQ(files->total_data_files, 5u);
}

TEST(CommitFilesTest, RejectsSnapshotsThatAreNotOurs) {
    // Another writer's append (different row count), a compaction, junk
    EXPECT_FALSE(commitFilesFromMetadata(kMetadata, 1499));
    std::string replace(kMetadata);
    replace.replace(replace.find("append"), 6, "replace");
    EXPECT_FALSE(commitFilesFromMetadata(replace, 1500));
    EXPECT_FALSE(commitFilesFromMetadata("{}", 1500));
    EXPECT_FALSE(commitFilesFromMetadata("not json", 1500));
}
