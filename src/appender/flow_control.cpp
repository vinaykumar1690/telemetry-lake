#include "flow_control.hpp"

#include <format>

namespace appender {

Result<std::map<std::int32_t, std::optional<std::int64_t>>> resumeOffsets(
    std::span<const std::int32_t> partitions,
    const Result<std::map<std::int32_t, std::int64_t>>& durable) {
    if (!durable) {
        return fatal(std::format("cannot read committed offsets: {}", durable.error().message));
    }
    std::map<std::int32_t, std::optional<std::int64_t>> out;
    for (auto partition : partitions) {
        auto it = durable->find(partition);
        out[partition] = it == durable->end() ? std::nullopt
                                              : std::optional<std::int64_t>(it->second + 1);
    }
    return out;
}

void FlowControl::offer(Work work, const Router& route) {
    auto it = parked_.find(work.partition);
    if (it != parked_.end() && !it->second.empty()) {
        it->second.push_back(std::move(work));  // never overtake a parked message
        return;
    }
    if (!route(work)) parked_[work.partition].push_back(std::move(work));
}

void FlowControl::drain(const Router& route) {
    for (auto it = parked_.begin(); it != parked_.end();) {
        auto& queue = it->second;
        while (!queue.empty() && route(queue.front())) queue.pop_front();
        it = queue.empty() ? parked_.erase(it) : std::next(it);
    }
}

std::set<std::int32_t> FlowControl::blocked() const {
    std::set<std::int32_t> out;
    for (const auto& [partition, queue] : parked_) {
        if (!queue.empty()) out.insert(partition);
    }
    return out;
}

void FlowControl::forget(const std::set<std::int32_t>& partitions) {
    for (auto p : partitions) parked_.erase(p);
}

std::size_t FlowControl::parkedCount() const {
    std::size_t total = 0;
    for (const auto& [partition, queue] : parked_) total += queue.size();
    return total;
}

}  // namespace appender
