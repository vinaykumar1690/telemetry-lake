#ifndef APPENDER_BACKOFF_HPP
#define APPENDER_BACKOFF_HPP

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <random>

namespace appender {

// base * 2^attempt, capped at max_delay; safe for any attempt count
constexpr std::chrono::milliseconds backoffCeiling(int attempt,
                                                   std::chrono::milliseconds base,
                                                   std::chrono::milliseconds max_delay) {
    if (base.count() <= 0 || max_delay <= base) return std::max(max_delay, base);
    std::int64_t delay = base.count();
    for (int i = 0; i < attempt && delay < max_delay.count(); ++i) {
        delay *= 2;  // stops at the cap, so it cannot overflow
    }
    return std::chrono::milliseconds{std::min<std::int64_t>(delay, max_delay.count())};
}

// Equal jitter: uniform in [ceiling / 2, ceiling], so retries never collapse to zero
template <typename Rng>
std::chrono::milliseconds backoffWithJitter(int attempt, std::chrono::milliseconds base,
                                            std::chrono::milliseconds max_delay, Rng& rng) {
    const auto ceiling = backoffCeiling(attempt, base, max_delay).count();
    std::uniform_int_distribution<std::int64_t> dist(ceiling / 2, ceiling);
    return std::chrono::milliseconds{dist(rng)};
}

}  // namespace appender

#endif  // APPENDER_BACKOFF_HPP
