#include "observability/metrics.hpp"

#include <prometheus/text_serializer.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>
#include <sys/resource.h>
#include <unistd.h>

#ifdef __APPLE__
#include <mach/mach.h>
#endif

#ifndef TELEMETRY_LAKE_VERSION
#define TELEMETRY_LAKE_VERSION "unknown"
#endif

namespace obs {
namespace {

using prometheus::MetricType;

// Close enough to the real start for a dashboard: static init runs before main
const double kProcessStart = std::chrono::duration<double>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();

std::optional<double> residentBytes() {
#ifdef __APPLE__
    mach_task_basic_info info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info),
                  &count) != KERN_SUCCESS) {
        return std::nullopt;
    }
    return static_cast<double>(info.resident_size);
#else
    std::ifstream statm("/proc/self/statm");
    long pages_total = 0;
    long pages_resident = 0;
    if (!(statm >> pages_total >> pages_resident)) return std::nullopt;
    return static_cast<double>(pages_resident) * static_cast<double>(::sysconf(_SC_PAGESIZE));
#endif
}

std::optional<double> threadCount() {
#ifdef __APPLE__
    thread_act_array_t threads = nullptr;
    mach_msg_type_number_t count = 0;
    if (task_threads(mach_task_self(), &threads, &count) != KERN_SUCCESS) return std::nullopt;
    for (mach_msg_type_number_t i = 0; i < count; ++i) mach_port_deallocate(mach_task_self(), threads[i]);
    vm_deallocate(mach_task_self(), reinterpret_cast<vm_address_t>(threads),
                  count * sizeof(thread_act_t));
    return static_cast<double>(count);
#else
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.starts_with("Threads:")) return std::stod(line.substr(8));
    }
    return std::nullopt;
#endif
}

std::optional<double> openFds() {
#ifdef __APPLE__
    const char* dir = "/dev/fd";
#else
    const char* dir = "/proc/self/fd";
#endif
    std::error_code ec;
    double count = 0;
    for (std::filesystem::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec)) {
        ++count;
    }
    if (ec) return std::nullopt;
    return count - 1;  // the descriptor of the directory being listed
}

}  // namespace

FamilyBuilder::FamilyBuilder(std::string name, std::string help, MetricType type) {
    family_.name = std::move(name);
    family_.help = std::move(help);
    family_.type = type;
}

FamilyBuilder& FamilyBuilder::add(double value, std::vector<prometheus::ClientMetric::Label> labels) {
    prometheus::ClientMetric metric;
    metric.label = std::move(labels);
    switch (family_.type) {
        case MetricType::Counter: metric.counter.value = value; break;
        case MetricType::Gauge: metric.gauge.value = value; break;
        default: metric.untyped.value = value; break;
    }
    family_.metric.push_back(std::move(metric));
    return *this;
}

prometheus::Histogram::BucketBoundaries exponentialBuckets(double start, double factor,
                                                           std::size_t count) {
    prometheus::Histogram::BucketBoundaries out;
    double bound = start;
    for (std::size_t i = 0; i < count; ++i, bound *= factor) out.push_back(bound);
    return out;
}

std::vector<prometheus::MetricFamily> processMetrics() {
    std::vector<prometheus::MetricFamily> out;
    rusage usage{};
    if (::getrusage(RUSAGE_SELF, &usage) == 0) {
        auto seconds = [](const timeval& tv) { return tv.tv_sec + tv.tv_usec / 1e6; };
        out.push_back(FamilyBuilder("process_cpu_seconds_total", "User and system CPU time",
                                    MetricType::Counter)
                          .add(seconds(usage.ru_utime) + seconds(usage.ru_stime))
                          .take());
    }
    if (auto rss = residentBytes()) {
        out.push_back(FamilyBuilder("process_resident_memory_bytes", "Resident memory",
                                    MetricType::Gauge).add(*rss).take());
    }
    if (auto fds = openFds()) {
        out.push_back(FamilyBuilder("process_open_fds", "Open file descriptors",
                                    MetricType::Gauge).add(*fds).take());
    }
    if (auto threads = threadCount()) {
        out.push_back(FamilyBuilder("process_threads", "OS threads", MetricType::Gauge)
                          .add(*threads).take());
    }
    out.push_back(FamilyBuilder("process_start_time_seconds", "Start time since the Unix epoch",
                                MetricType::Gauge).add(kProcessStart).take());
    return out;
}

Metrics::Metrics(std::string service) : registry_(std::make_shared<prometheus::Registry>()) {
    addCollectable(std::make_shared<CallbackCollectable>([service] {
        auto families = processMetrics();
        families.push_back(FamilyBuilder("telemetry_lake_build_info",
                                         "Always 1; labels identify the build", MetricType::Gauge)
                               .add(1, {{"service", service}, {"version", TELEMETRY_LAKE_VERSION}})
                               .take());
        return families;
    }));
}

void Metrics::addCollectable(std::shared_ptr<prometheus::Collectable> collectable) {
    std::lock_guard lock(mutex_);
    collectables_.push_back(std::move(collectable));
}

void Metrics::onScrape(std::function<void()> hook) {
    std::lock_guard lock(mutex_);
    hooks_.push_back(std::move(hook));
}

std::string Metrics::render() const {
    std::vector<std::shared_ptr<prometheus::Collectable>> collectables;
    std::vector<std::function<void()>> hooks;
    {
        std::lock_guard lock(mutex_);
        collectables = collectables_;
        hooks = hooks_;
    }
    for (const auto& hook : hooks) hook();

    auto families = registry_->Collect();
    for (const auto& collectable : collectables) {
        for (auto& family : collectable->Collect()) {
            // An empty family would still print HELP/TYPE lines
            if (!family.metric.empty()) families.push_back(std::move(family));
        }
    }
    return prometheus::TextSerializer().Serialize(families);
}

}  // namespace obs
