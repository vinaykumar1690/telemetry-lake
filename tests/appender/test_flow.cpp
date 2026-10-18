#include "appender/flow_control.hpp"

#include <gtest/gtest.h>

using namespace appender;

namespace {

Work work(std::int32_t partition, std::int64_t offset) {
    return Work{partition, offset, {}, {}, nullptr};
}

}  // namespace

TEST(ResumeOffsetsTest, OnePastLastDurableOffset) {
    const std::int32_t partitions[] = {0, 1, 2};
    Result<std::map<std::int32_t, std::int64_t>> durable = std::map<std::int32_t, std::int64_t>{
        {0, 41}, {2, 0}, {7, 99}};
    auto resume = resumeOffsets(partitions, durable);
    ASSERT_TRUE(resume);
    EXPECT_EQ(resume->size(), 3u);
    EXPECT_EQ(resume->at(0), 42);
    EXPECT_EQ(resume->at(1), std::nullopt);  // never committed: from the beginning
    EXPECT_EQ(resume->at(2), 1);
}

TEST(ResumeOffsetsTest, StoreErrorIsFatal) {
    const std::int32_t partitions[] = {0};
    Result<std::map<std::int32_t, std::int64_t>> durable = retryable("catalog down");
    auto resume = resumeOffsets(partitions, durable);
    ASSERT_FALSE(resume);
    EXPECT_EQ(resume.error().kind, ErrorKind::Fatal);
}

TEST(FlowControlTest, ParksAndPreservesOrder) {
    FlowControl flow;
    std::vector<std::pair<int, std::int64_t>> routed;
    bool open = false;
    FlowControl::Router route = [&](Work& w) {
        if (!open && w.partition == 0) return false;
        routed.emplace_back(w.partition, w.offset);
        return true;
    };

    flow.offer(work(0, 1), route);  // parked
    flow.offer(work(1, 1), route);  // other partitions flow
    flow.offer(work(0, 2), route);  // queues behind offset 1
    EXPECT_EQ(flow.blocked(), std::set<std::int32_t>{0});
    EXPECT_EQ(flow.parkedCount(), 2u);
    EXPECT_EQ(routed, (std::vector<std::pair<int, std::int64_t>>{{1, 1}}));

    open = true;
    flow.drain(route);
    EXPECT_TRUE(flow.blocked().empty());
    EXPECT_EQ(routed, (std::vector<std::pair<int, std::int64_t>>{{1, 1}, {0, 1}, {0, 2}}));
}

TEST(FlowControlTest, DrainStopsAtFirstFailure) {
    FlowControl flow;
    int budget = 0;
    FlowControl::Router route = [&](Work&) { return budget-- > 0; };
    for (int offset = 0; offset < 3; ++offset) flow.offer(work(0, offset), route);
    budget = 1;
    flow.drain(route);
    EXPECT_EQ(flow.parkedCount(), 2u);
}

TEST(FlowControlTest, ForgetDropsParkedMessages) {
    FlowControl flow;
    FlowControl::Router reject = [](Work&) { return false; };
    flow.offer(work(0, 1), reject);
    flow.offer(work(1, 1), reject);
    flow.forget({0});
    EXPECT_EQ(flow.blocked(), std::set<std::int32_t>{1});
}
