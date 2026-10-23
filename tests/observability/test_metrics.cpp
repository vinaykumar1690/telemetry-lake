#include "observability/metrics.hpp"

#include <gtest/gtest.h>

namespace {

TEST(Metrics, RendersRegistryCollectablesAndProcess) {
    obs::Metrics metrics("test-service");
    auto& requests = prometheus::BuildCounter()
                         .Name("test_requests_total")
                         .Help("Requests")
                         .Register(metrics.registry())
                         .Add({{"code", "200"}});
    requests.Increment(3);

    int scrapes = 0;
    metrics.onScrape([&] { ++scrapes; });
    metrics.addCollectable(std::make_shared<obs::CallbackCollectable>([] {
        std::vector<prometheus::MetricFamily> out;
        out.push_back(obs::FamilyBuilder("test_live", "Live value", prometheus::MetricType::Gauge)
                          .add(7, {{"partition", "1"}})
                          .take());
        // Empty families are left out of the output
        out.push_back(obs::FamilyBuilder("test_empty", "Nothing", prometheus::MetricType::Gauge).take());
        return out;
    }));

    const auto text = metrics.render();
    EXPECT_EQ(scrapes, 1);
    EXPECT_NE(text.find(R"(test_requests_total{code="200"} 3)"), std::string::npos) << text;
    EXPECT_NE(text.find(R"(test_live{partition="1"} 7)"), std::string::npos);
    EXPECT_EQ(text.find("test_empty"), std::string::npos);
    EXPECT_NE(text.find(R"(telemetry_lake_build_info{service="test-service",version=)"), std::string::npos);
    EXPECT_NE(text.find("process_cpu_seconds_total"), std::string::npos);
    EXPECT_NE(text.find("process_resident_memory_bytes"), std::string::npos);
    EXPECT_NE(text.find("process_open_fds"), std::string::npos);
    EXPECT_NE(text.find("process_start_time_seconds"), std::string::npos);
}

TEST(Metrics, ExponentialBuckets) {
    const auto buckets = obs::exponentialBuckets(0.001, 10, 4);
    const prometheus::Histogram::BucketBoundaries expected{0.001, 0.01, 0.1, 1};
    ASSERT_EQ(buckets.size(), expected.size());
    for (std::size_t i = 0; i < expected.size(); ++i) EXPECT_DOUBLE_EQ(buckets[i], expected[i]);
}

}  // namespace
