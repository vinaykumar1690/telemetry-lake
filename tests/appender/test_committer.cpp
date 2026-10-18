#include "appender/iceberg_committer.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <deque>
#include <format>
#include <latch>

using namespace appender;
using namespace std::chrono_literals;

namespace {

// Staged data the test controls; each sealAll hands out what was staged
class FakeSource : public StagingSource {
public:
    void stage(std::int32_t partition, std::int64_t max_offset, std::size_t rows = 1,
               std::size_t bytes = 10) {
        std::lock_guard lock(mutex_);
        staged_[partition] = {partition, std::format("t{}", next_table_++), max_offset, rows, bytes};
        oldest_ = std::chrono::steady_clock::now();
    }

    Result<std::vector<SealedBuffer>> sealAll(const std::set<std::int32_t>& excluded) override {
        std::lock_guard lock(mutex_);
        std::vector<SealedBuffer> out;
        for (auto it = staged_.begin(); it != staged_.end();) {
            if (excluded.contains(it->first)) {
                ++it;
                continue;
            }
            out.push_back(it->second);
            it = staged_.erase(it);
        }
        if (staged_.empty()) oldest_.reset();
        return out;
    }
    void dropSealed(const SealedBuffer& sealed) override {
        std::lock_guard lock(mutex_);
        dropped_.push_back(sealed.table);
    }
    void discard(const std::set<std::int32_t>& partitions) override {
        std::lock_guard lock(mutex_);
        for (auto p : partitions) staged_.erase(p);
        discarded_.insert(partitions.begin(), partitions.end());
    }
    std::size_t unsealedBytes() const override {
        std::lock_guard lock(mutex_);
        std::size_t total = 0;
        for (const auto& [p, b] : staged_) total += b.bytes;
        return total;
    }
    std::optional<std::chrono::steady_clock::time_point> oldestUnsealedRow() const override {
        std::lock_guard lock(mutex_);
        return oldest_;
    }

    std::vector<std::string> dropped() const {
        std::lock_guard lock(mutex_);
        return dropped_;
    }
    std::set<std::int32_t> discarded() const {
        std::lock_guard lock(mutex_);
        return discarded_;
    }

private:
    mutable std::mutex mutex_;
    std::map<std::int32_t, SealedBuffer> staged_;
    std::optional<std::chrono::steady_clock::time_point> oldest_;
    std::vector<std::string> dropped_;
    std::set<std::int32_t> discarded_;
    int next_table_ = 0;
};

// Records commits; scripted results are consumed first, then success
class FakeSink : public IcebergSink {
public:
    Result<void> commit(std::span<const SealedBuffer> buffers, const CommitContext&) override {
        if (gate_) gate_->arrive_and_wait();
        std::lock_guard lock(mutex_);
        ++calls_;
        Result<void> result{};
        if (!scripted_.empty()) {
            result = scripted_.front();
            scripted_.pop_front();
        }
        if (result || result.error().kind == ErrorKind::Indeterminate && land_on_indeterminate_) {
            for (const auto& b : buffers) {
                auto& offset = durable_[b.partition];
                offset = std::max(offset, b.max_offset);
                rows_ += b.rows;
            }
        }
        return result;
    }

    void script(Result<void> result) {
        std::lock_guard lock(mutex_);
        scripted_.push_back(std::move(result));
    }
    int calls() const {
        std::lock_guard lock(mutex_);
        return calls_;
    }
    std::size_t rows() const {
        std::lock_guard lock(mutex_);
        return rows_;
    }
    std::map<std::int32_t, std::int64_t> durable() const {
        std::lock_guard lock(mutex_);
        return durable_;
    }

    bool land_on_indeterminate_ = false;
    std::latch* gate_ = nullptr;

private:
    mutable std::mutex mutex_;
    std::deque<Result<void>> scripted_;
    std::map<std::int32_t, std::int64_t> durable_;
    std::size_t rows_ = 0;
    int calls_ = 0;
};

// Reads what the fake sink made durable
class FakeStore : public OffsetStore {
public:
    explicit FakeStore(FakeSink& sink) : sink_(sink) {}
    Result<std::map<std::int32_t, std::int64_t>> lastCommitted(
        std::span<const std::int32_t> partitions) override {
        ++reads;
        if (fail) return retryable("store down");
        std::map<std::int32_t, std::int64_t> out;
        for (auto [p, o] : sink_.durable()) {
            if (std::ranges::find(partitions, p) != partitions.end()) out[p] = o;
        }
        return out;
    }
    Result<void> prune() override {
        ++prunes;
        return {};
    }
    std::atomic<int> reads = 0;
    std::atomic<int> prunes = 0;
    std::atomic<bool> fail = false;

private:
    FakeSink& sink_;
};

CommitterOptions fastOptions() {
    CommitterOptions options;
    options.flush_bytes = 1000;
    options.flush_interval = 10s;
    options.commit_retries = 3;
    options.retry_base_delay = 1ms;
    options.retry_max_delay = 5ms;
    options.maintenance_interval = 0ms;
    options.poll_interval = 5ms;
    return options;
}

struct Harness {
    explicit Harness(CommitterOptions options = fastOptions())
        : store(sink),
          committer(source, sink, store, options,
                    [this](const auto& offsets) {
                        std::lock_guard lock(mutex);
                        for (auto [p, o] : offsets) reported[p] = o;
                    },
                    [this](const AppenderError&) { fatal = true; }) {}

    std::map<std::int32_t, std::int64_t> reportedOffsets() {
        std::lock_guard lock(mutex);
        return reported;
    }

    FakeSource source;
    FakeSink sink;
    FakeStore store;
    std::mutex mutex;
    std::map<std::int32_t, std::int64_t> reported;
    std::atomic<bool> fatal = false;
    IcebergCommitter committer;
};

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

TEST(IcebergCommitterTest, FlushRequestCommitsAndReportsOffsets) {
    Harness h;
    h.source.stage(0, 41, 3);
    h.source.stage(1, 7, 2);
    h.committer.start();
    auto result = h.committer.requestFlush().get();
    ASSERT_TRUE(result) << result.error().message;
    EXPECT_EQ(h.sink.rows(), 5u);
    EXPECT_EQ(h.reportedOffsets(), (std::map<std::int32_t, std::int64_t>{{0, 41}, {1, 7}}));
    EXPECT_EQ(h.source.dropped().size(), 2u);
    EXPECT_EQ(h.committer.stats().commits, 1u);
}

TEST(IcebergCommitterTest, FlushWithNothingStagedSucceeds) {
    Harness h;
    h.committer.start();
    EXPECT_TRUE(h.committer.requestFlush().get());
    EXPECT_EQ(h.sink.calls(), 0);
}

TEST(IcebergCommitterTest, SizeThresholdTriggersCommit) {
    Harness h;
    h.committer.start();
    h.source.stage(0, 1, 1, 2000);  // above flush_bytes
    EXPECT_TRUE(eventually([&] { return h.sink.calls() == 1; }));
}

TEST(IcebergCommitterTest, AgeThresholdTriggersCommit) {
    auto options = fastOptions();
    options.flush_interval = 20ms;
    Harness h(options);
    h.committer.start();
    h.source.stage(0, 1, 1, 10);
    EXPECT_FALSE(eventually([&] { return h.sink.calls() > 0; }, 5ms));
    EXPECT_TRUE(eventually([&] { return h.sink.calls() == 1; }));
}

TEST(IcebergCommitterTest, RetryableFailureIsRetried) {
    Harness h;
    h.sink.script(retryable("409 conflict"));
    h.source.stage(0, 5);
    h.committer.start();
    EXPECT_TRUE(h.committer.requestFlush().get());
    EXPECT_EQ(h.sink.calls(), 2);
    EXPECT_EQ(h.reportedOffsets().at(0), 5);
    EXPECT_EQ(h.store.reads, 0);  // a definitive failure needs no verification
}

TEST(IcebergCommitterTest, IndeterminateThatLandedIsNotWrittenTwice) {
    Harness h;
    h.sink.land_on_indeterminate_ = true;
    h.sink.script(indeterminate("timeout waiting for catalog"));
    h.source.stage(0, 9, 4);
    h.committer.start();
    auto result = h.committer.requestFlush().get();
    ASSERT_TRUE(result) << result.error().message;
    EXPECT_EQ(h.sink.calls(), 1);  // verification found it durable
    EXPECT_EQ(h.sink.rows(), 4u);
    EXPECT_EQ(h.store.reads, 1);
    EXPECT_EQ(h.reportedOffsets().at(0), 9);
    EXPECT_EQ(h.source.dropped().size(), 1u);
}

TEST(IcebergCommitterTest, IndeterminateThatDidNotLandIsRetried) {
    Harness h;
    h.sink.script(indeterminate("connection reset"));
    h.source.stage(0, 9, 4);
    h.committer.start();
    ASSERT_TRUE(h.committer.requestFlush().get());
    EXPECT_EQ(h.sink.calls(), 2);
    EXPECT_EQ(h.sink.rows(), 4u);
    EXPECT_EQ(h.store.reads, 1);
}

TEST(IcebergCommitterTest, UnverifiableIndeterminateNeverRecommits) {
    Harness h;
    h.sink.script(indeterminate("timeout"));
    h.store.fail = true;
    h.source.stage(0, 9);
    h.committer.start();
    auto result = h.committer.requestFlush().get();
    ASSERT_FALSE(result);
    EXPECT_EQ(h.sink.calls(), 1);  // never re-sent without knowing the outcome

    // Once the store is reachable the next cycle verifies before committing
    h.store.fail = false;
    ASSERT_TRUE(h.committer.requestFlush().get());
    EXPECT_EQ(h.sink.calls(), 2);
    EXPECT_EQ(h.reportedOffsets().at(0), 9);
}

TEST(IcebergCommitterTest, ExhaustedRetriesKeepDataForTheNextCycle) {
    Harness h;
    for (int i = 0; i < 3; ++i) h.sink.script(retryable("catalog down"));
    h.source.stage(0, 3);
    h.committer.start();
    EXPECT_FALSE(h.committer.requestFlush().get());
    EXPECT_EQ(h.committer.stats().consecutive_failures, 1u);
    h.source.stage(0, 8);  // more data arrives; both generations go together
    ASSERT_TRUE(h.committer.requestFlush().get());
    EXPECT_EQ(h.reportedOffsets().at(0), 8);
    EXPECT_EQ(h.source.dropped().size(), 2u);
    EXPECT_EQ(h.committer.stats().consecutive_failures, 0u);
}

TEST(IcebergCommitterTest, FatalFailureStopsCommitting) {
    Harness h;
    h.sink.script(fatal("schema mismatch"));
    h.source.stage(0, 3);
    h.committer.start();
    auto result = h.committer.requestFlush().get();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().kind, ErrorKind::Fatal);
    EXPECT_TRUE(h.fatal);
    EXPECT_EQ(h.sink.calls(), 1);
    EXPECT_TRUE(h.committer.stats().fatal);
    EXPECT_FALSE(h.committer.requestFlush().get());
}

TEST(IcebergCommitterTest, FenceWaitsForInFlightCommit) {
    Harness h;
    std::latch gate(2);
    h.sink.gate_ = &gate;
    h.source.stage(3, 10);
    h.committer.start();
    auto flushed = h.committer.requestFlush();
    ASSERT_TRUE(eventually([&] { return h.committer.stats().commits == 0; }));
    std::this_thread::sleep_for(20ms);  // the commit is now blocked in the sink

    // While the commit is blocked the fence cannot complete
    EXPECT_FALSE(h.committer.fence({3}, std::chrono::steady_clock::now() + 30ms));
    gate.arrive_and_wait();  // let the commit finish
    EXPECT_TRUE(flushed.get());
    EXPECT_TRUE(h.committer.fence({3}, std::chrono::steady_clock::now() + 1s));
}

TEST(IcebergCommitterTest, FencedPartitionsAreNotCommittedAndCanBeDiscarded) {
    Harness h;
    h.committer.start();
    ASSERT_TRUE(h.committer.fence({1}, std::chrono::steady_clock::now() + 1s));
    h.source.stage(0, 5);
    h.source.stage(1, 6);
    ASSERT_TRUE(h.committer.requestFlush().get());
    EXPECT_EQ(h.sink.durable(), (std::map<std::int32_t, std::int64_t>{{0, 5}}));
    h.committer.discard({1});
    EXPECT_EQ(h.source.discarded(), (std::set<std::int32_t>{1}));

    h.committer.include({1});
    h.source.stage(1, 9);
    ASSERT_TRUE(h.committer.requestFlush().get());
    EXPECT_EQ(h.sink.durable().at(1), 9);
}

TEST(IcebergCommitterTest, FenceInterruptsRetryBackoff) {
    auto options = fastOptions();
    options.retry_base_delay = 5s;
    options.retry_max_delay = 5s;
    Harness h(options);
    h.sink.script(retryable("down"));
    h.source.stage(2, 4);
    h.committer.start();
    auto flushed = h.committer.requestFlush();
    ASSERT_TRUE(eventually([&] { return h.sink.calls() == 1; }));
    const auto started = std::chrono::steady_clock::now();
    EXPECT_TRUE(h.committer.fence({2}, started + 2s));
    EXPECT_LT(std::chrono::steady_clock::now() - started, 1s);
    EXPECT_FALSE(flushed.get());
    h.committer.discard({2});
    EXPECT_EQ(h.source.dropped().size(), 1u);  // the pending buffer was released
}

TEST(IcebergCommitterTest, StopCommitsWhatIsStaged) {
    Harness h;
    h.committer.start();
    h.source.stage(0, 77);
    h.committer.stop();
    EXPECT_EQ(h.sink.durable().at(0), 77);
}

TEST(IcebergCommitterTest, MaintenanceRunsAfterSuccessfulCommits) {
    auto options = fastOptions();
    options.maintenance_interval = 1ms;
    Harness h(options);
    h.source.stage(0, 1);
    h.committer.start();
    std::this_thread::sleep_for(5ms);
    ASSERT_TRUE(h.committer.requestFlush().get());
    EXPECT_GE(h.store.prunes, 1);
}
