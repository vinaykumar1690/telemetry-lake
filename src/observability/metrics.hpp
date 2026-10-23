#ifndef OBSERVABILITY_METRICS_HPP
#define OBSERVABILITY_METRICS_HPP

#include <prometheus/collectable.h>
#include <prometheus/counter.h>
#include <prometheus/family.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/metric_family.h>
#include <prometheus/registry.h>

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

// Prometheus metrics: a registry for metrics updated as things happen, plus
// collectables whose values are read when /metrics is scraped

namespace obs {

class Metrics {
public:
    // Registers build_info and the process metrics
    explicit Metrics(std::string service);

    prometheus::Registry& registry() { return *registry_; }

    // Values produced at scrape time; the caller keeps the collectable alive
    void addCollectable(std::shared_ptr<prometheus::Collectable> collectable);

    // Runs before every scrape (e.g. to poll a client so its stats are fresh)
    void onScrape(std::function<void()> hook);

    // Prometheus text exposition format
    std::string render() const;

private:
    std::shared_ptr<prometheus::Registry> registry_;
    mutable std::mutex mutex_;
    std::vector<std::shared_ptr<prometheus::Collectable>> collectables_;
    std::vector<std::function<void()>> hooks_;
};

// Builds metric families by hand inside Collect()
class FamilyBuilder {
public:
    FamilyBuilder(std::string name, std::string help, prometheus::MetricType type);

    FamilyBuilder& add(double value, std::vector<prometheus::ClientMetric::Label> labels = {});
    bool empty() const { return family_.metric.empty(); }
    prometheus::MetricFamily take() { return std::move(family_); }

private:
    prometheus::MetricFamily family_;
};

// A Collectable over a function, for values that already live elsewhere
class CallbackCollectable : public prometheus::Collectable {
public:
    using Fn = std::function<std::vector<prometheus::MetricFamily>()>;
    explicit CallbackCollectable(Fn fn) : fn_(std::move(fn)) {}
    std::vector<prometheus::MetricFamily> Collect() const override { return fn_(); }

private:
    Fn fn_;
};

// Exponential bucket boundaries: start, start*factor, ... (count values)
prometheus::Histogram::BucketBoundaries exponentialBuckets(double start, double factor,
                                                           std::size_t count);

// Process CPU, memory, file descriptors, threads and start time
std::vector<prometheus::MetricFamily> processMetrics();

}  // namespace obs

#endif  // OBSERVABILITY_METRICS_HPP
