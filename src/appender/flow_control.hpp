#ifndef APPENDER_FLOW_CONTROL_HPP
#define APPENDER_FLOW_CONTROL_HPP

#include "error.hpp"
#include "pipeline_pool.hpp"

#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <span>

namespace appender {

// Where to resume each assigned partition: one past the last offset the
// offsets table records, or nullopt (start from the beginning) if none.
// Fatal if the table cannot be read: guessing could duplicate or lose data.
Result<std::map<std::int32_t, std::optional<std::int64_t>>> resumeOffsets(
    std::span<const std::int32_t> partitions,
    const Result<std::map<std::int32_t, std::int64_t>>& durable);

// Keeps per-partition order when the pipeline is full. A message that cannot
// be routed is parked, and every later message of its partition queues
// behind it; such partitions should be paused at the consumer until their
// parked messages drain.
class FlowControl {
public:
    using Router = std::function<bool(Work&)>;

    void offer(Work work, const Router& route);
    // Retries parked messages in order; stops at a partition's first failure
    void drain(const Router& route);
    // Partitions that have parked messages
    std::set<std::int32_t> blocked() const;
    void forget(const std::set<std::int32_t>& partitions);
    void clear() { parked_.clear(); }
    std::size_t parkedCount() const;

private:
    std::map<std::int32_t, std::deque<Work>> parked_;
};

}  // namespace appender

#endif  // APPENDER_FLOW_CONTROL_HPP
