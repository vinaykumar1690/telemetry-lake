#include "appender/pipeline_pool.hpp"
#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"
#include "telemetry_wrapper.pb.h"

#include <gtest/gtest.h>

#include <atomic>
#include <deque>
#include <map>
#include <numeric>

using namespace appender;
using namespace std::chrono_literals;

namespace {

std::string logsPayload(int records) {
    opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest request;
    auto* scope_logs = request.add_resource_logs()->add_scope_logs();
    for (int i = 0; i < records; ++i) {
        scope_logs->add_log_records()->mutable_body()->set_string_value(std::to_string(i));
    }
    telemetry::v1::RawTelemetryMessage wrapper;
    wrapper.set_content_type("application/x-protobuf");
    wrapper.set_telemetry_type(telemetry::v1::OTEL_LOGS);
    wrapper.set_payload(request.SerializeAsString());
    return wrapper.SerializeAsString();
}

Work makeWork(std::int32_t partition, std::int64_t offset, std::string payload) {
    auto owner = std::make_shared<std::string>(std::move(payload));
    return Work{partition, offset, {}, *owner, owner};
}

class FakeWriter : public PartitionWriter {
public:
    Result<void> append(std::span<const LogRow> rows, std::int64_t max_offset,
                        std::size_t) override {
        std::lock_guard lock(mutex);
        if (failures_left > 0) {
            --failures_left;
            return std::unexpected(AppenderError{failure_kind, "staging down"});
        }
        offsets.push_back(max_offset);
        row_count += rows.size();
        return {};
    }
    void advanceOffset(std::int64_t offset) override {
        std::lock_guard lock(mutex);
        offsets.push_back(offset);
        advanced.push_back(offset);
    }
    std::vector<std::int64_t> snapshot() {
        std::lock_guard lock(mutex);
        return offsets;
    }

    std::mutex mutex;
    std::vector<std::int64_t> offsets;  // every offset the partition moved to, in order
    std::vector<std::int64_t> advanced;
    std::size_t row_count = 0;
    int failures_left = 0;
    ErrorKind failure_kind = ErrorKind::Retryable;
};

class FakeStaging : public StagingLookup {
public:
    std::shared_ptr<PartitionWriter> writer(std::int32_t partition) override {
        std::lock_guard lock(mutex);
        auto it = writers.find(partition);
        return it == writers.end() ? nullptr : it->second;
    }
    bool waitForCapacity(std::chrono::milliseconds timeout) override {
        if (full) std::this_thread::sleep_for(timeout);
        return !full;
    }
    std::shared_ptr<FakeWriter> add(std::int32_t partition) {
        auto writer = std::make_shared<FakeWriter>();
        std::lock_guard lock(mutex);
        writers[partition] = writer;
        return writer;
    }

    std::mutex mutex;
    std::map<std::int32_t, std::shared_ptr<FakeWriter>> writers;
    std::atomic<bool> full = false;
};

class FakeDlq : public DlqSink {
public:
    Result<void> send(const DlqRecord& record) override {
        std::lock_guard lock(mutex);
        if (fail) return retryable("broker down");
        sent.push_back(record.source_offset);
        return {};
    }
    std::vector<std::int64_t> snapshot() {
        std::lock_guard lock(mutex);
        return sent;
    }
    std::mutex mutex;
    std::vector<std::int64_t> sent;
    std::atomic<bool> fail = false;
};

PipelineOptions options(std::size_t threads = 2, std::size_t queue_bytes = 1 << 20) {
    return {.threads = threads, .queue_bytes = queue_bytes, .topic = "otel-logs",
            .retry_base_delay = 1ms, .retry_max_delay = 5ms, .max_append_attempts = 3};
}

template <typename Pred>
bool eventually(Pred pred, std::chrono::milliseconds timeout = 2s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(2ms);
    }
    return pred();
}

}  // namespace

TEST(PipelinePoolTest, StagesMessagesInOffsetOrderPerPartition) {
    FakeStaging staging;
    auto p0 = staging.add(0);
    auto p1 = staging.add(1);
    FakeDlq dlq;
    PipelinePool pool(options(), staging, dlq, nullptr);
    pool.start();
    for (int offset = 0; offset < 50; ++offset) {
        for (int partition : {0, 1}) {
            auto work = makeWork(partition, offset, logsPayload(2));
            ASSERT_TRUE(pool.tryRoute(work));
        }
    }
    pool.stop();  // drains
    std::vector<std::int64_t> expected(50);
    std::iota(expected.begin(), expected.end(), 0);
    EXPECT_EQ(p0->snapshot(), expected);
    EXPECT_EQ(p1->snapshot(), expected);
    EXPECT_EQ(p0->row_count, 100u);
    EXPECT_EQ(pool.stats().messages, 100u);
    EXPECT_EQ(pool.stats().rows, 200u);
}

TEST(PipelinePoolTest, EmptyRequestStillAdvancesOffset) {
    FakeStaging staging;
    auto p0 = staging.add(0);
    FakeDlq dlq;
    PipelinePool pool(options(1), staging, dlq, nullptr);
    pool.start();
    auto work = makeWork(0, 7, logsPayload(0));
    ASSERT_TRUE(pool.tryRoute(work));
    pool.stop();
    EXPECT_EQ(p0->snapshot(), std::vector<std::int64_t>{7});
}

TEST(PipelinePoolTest, PoisonGoesToDlqThenOffsetAdvances) {
    FakeStaging staging;
    auto p0 = staging.add(0);
    FakeDlq dlq;
    PipelinePool pool(options(1), staging, dlq, nullptr);
    pool.start();
    for (auto [offset, payload] : std::vector<std::pair<int, std::string>>{
             {1, logsPayload(1)}, {2, "not a wrapper \xff\xff"}, {3, logsPayload(1)}}) {
        auto work = makeWork(0, offset, payload);
        ASSERT_TRUE(pool.tryRoute(work));
    }
    pool.stop();
    EXPECT_EQ(dlq.snapshot(), std::vector<std::int64_t>{2});
    EXPECT_EQ(p0->snapshot(), (std::vector<std::int64_t>{1, 2, 3}));
    EXPECT_EQ(p0->advanced, std::vector<std::int64_t>{2});
    EXPECT_EQ(pool.stats().poison, 1u);
}

TEST(PipelinePoolTest, DlqOutageBlocksThePartitionUntilItRecovers) {
    FakeStaging staging;
    auto p0 = staging.add(0);
    FakeDlq dlq;
    dlq.fail = true;
    PipelinePool pool(options(1), staging, dlq, nullptr);
    pool.start();
    auto poison = makeWork(0, 1, "garbage");
    auto next = makeWork(0, 2, logsPayload(1));
    ASSERT_TRUE(pool.tryRoute(poison));
    ASSERT_TRUE(pool.tryRoute(next));
    ASSERT_TRUE(eventually([&] { return pool.stats().dlq_failures >= 3; }));
    EXPECT_TRUE(p0->snapshot().empty());  // nothing moves past the poison message
    dlq.fail = false;
    ASSERT_TRUE(eventually([&] { return p0->snapshot().size() == 2; }));
    EXPECT_EQ(p0->snapshot(), (std::vector<std::int64_t>{1, 2}));
    pool.stop();
}

TEST(PipelinePoolTest, AbortDuringDlqOutageSkipsLaterMessagesOfThatPartition) {
    FakeStaging staging;
    auto p0 = staging.add(0);
    FakeDlq dlq;
    dlq.fail = true;
    PipelinePool pool(options(1), staging, dlq, nullptr);
    pool.start();
    auto poison = makeWork(0, 1, "garbage");
    auto next = makeWork(0, 2, logsPayload(1));
    ASSERT_TRUE(pool.tryRoute(poison));
    ASSERT_TRUE(pool.tryRoute(next));
    ASSERT_TRUE(eventually([&] { return pool.stats().dlq_failures >= 1; }));
    pool.stop(/*abort=*/true);
    EXPECT_TRUE(p0->snapshot().empty());  // offset 2 must not be staged past offset 1
}

TEST(PipelinePoolTest, RetryableStagingFailureIsRetried) {
    FakeStaging staging;
    auto p0 = staging.add(0);
    p0->failures_left = 2;
    FakeDlq dlq;
    PipelinePool pool(options(1), staging, dlq, nullptr);
    pool.start();
    auto work = makeWork(0, 5, logsPayload(1));
    ASSERT_TRUE(pool.tryRoute(work));
    pool.stop();
    EXPECT_EQ(p0->snapshot(), std::vector<std::int64_t>{5});
}

TEST(PipelinePoolTest, FatalStagingFailureIsReportedAndBlocksPartition) {
    FakeStaging staging;
    auto p0 = staging.add(0);
    p0->failures_left = 1;
    p0->failure_kind = ErrorKind::Fatal;
    FakeDlq dlq;
    std::atomic<int> fatals = 0;
    PipelinePool pool(options(1), staging, dlq, [&](const AppenderError&) { ++fatals; });
    pool.start();
    auto first = makeWork(0, 1, logsPayload(1));
    auto second = makeWork(0, 2, logsPayload(1));
    ASSERT_TRUE(pool.tryRoute(first));
    ASSERT_TRUE(pool.tryRoute(second));
    pool.stop();
    EXPECT_EQ(fatals, 1);
    EXPECT_TRUE(p0->snapshot().empty());
}

TEST(PipelinePoolTest, FullQueueRejectsWithoutConsumingWork) {
    FakeStaging staging;
    staging.add(0);
    staging.full = true;  // the thread blocks on capacity, so the queue fills
    FakeDlq dlq;
    const auto payload = logsPayload(1);
    PipelinePool pool(options(1, payload.size() * 3), staging, dlq, nullptr);
    pool.start();
    int accepted = 0;
    for (int offset = 0; offset < 10; ++offset) {
        auto work = makeWork(0, offset, payload);
        if (!pool.tryRoute(work)) {
            EXPECT_FALSE(work.payload.empty());  // still owned by the caller
            break;
        }
        ++accepted;
    }
    EXPECT_LT(accepted, 10);
    EXPECT_FALSE(pool.hasRoom(0));
    staging.full = false;
    ASSERT_TRUE(eventually([&] { return pool.hasRoom(0); }));
    pool.stop();
}

TEST(PipelinePoolTest, DropPartitionsRemovesQueuedWorkAndInterruptsRetries) {
    FakeStaging staging;
    auto p0 = staging.add(0);
    staging.add(1);
    FakeDlq dlq;
    dlq.fail = true;
    PipelinePool pool(options(1), staging, dlq, nullptr);
    pool.start();
    auto poison = makeWork(0, 1, "garbage");
    ASSERT_TRUE(pool.tryRoute(poison));
    for (int offset = 2; offset < 5; ++offset) {
        auto work = makeWork(0, offset, logsPayload(1));
        ASSERT_TRUE(pool.tryRoute(work));
    }
    ASSERT_TRUE(eventually([&] { return pool.stats().dlq_failures >= 1; }));
    pool.dropPartitions({0});  // returns once the stuck thread let go
    EXPECT_TRUE(p0->snapshot().empty());

    // The thread serves other partitions afterwards, and partition 0 again if reassigned
    dlq.fail = false;
    auto other = makeWork(1, 9, logsPayload(1));
    ASSERT_TRUE(pool.tryRoute(other));
    auto again = makeWork(0, 1, logsPayload(1));
    ASSERT_TRUE(pool.tryRoute(again));
    pool.stop();
    EXPECT_EQ(staging.writers[1]->snapshot(), std::vector<std::int64_t>{9});
    EXPECT_EQ(p0->snapshot(), std::vector<std::int64_t>{1});
}

TEST(PipelinePoolTest, UnassignedPartitionIsIgnored) {
    FakeStaging staging;
    FakeDlq dlq;
    PipelinePool pool(options(1), staging, dlq, nullptr);
    pool.start();
    auto work = makeWork(5, 1, logsPayload(1));
    ASSERT_TRUE(pool.tryRoute(work));
    pool.stop();
    EXPECT_EQ(pool.stats().messages, 0u);
}
