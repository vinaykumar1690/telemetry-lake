#ifndef APPENDER_BOUNDED_QUEUE_HPP
#define APPENDER_BOUNDED_QUEUE_HPP

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <optional>
#include <utility>

namespace appender {

struct UnitWeight {
    template <typename T>
    std::size_t operator()(const T&) const { return 1; }
};

// Multi-producer queue bounded by the summed weight of its items (e.g. bytes).
// An item heavier than the whole capacity is still accepted when the queue is
// empty, so oversized messages slow the pipeline down instead of wedging it.
template <typename T, typename Weigh = UnitWeight>
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity, Weigh weigh = {})
        : capacity_(capacity), weigh_(std::move(weigh)) {}

    BoundedQueue(const BoundedQueue&) = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;

    // Moves from item only on success; on false (full or closed) it is untouched
    bool try_push(T& item) {
        const std::size_t w = weigh_(item);
        {
            std::lock_guard lock(mutex_);
            if (closed_ || !fits(w)) return false;
            weight_ += w;
            items_.emplace_back(std::move(item), w);
        }
        not_empty_.notify_one();
        return true;
    }

    // Blocks until there is room; false if the queue was closed
    bool push_wait(T item) {
        const std::size_t w = weigh_(item);
        {
            std::unique_lock lock(mutex_);
            not_full_.wait(lock, [&] { return closed_ || fits(w); });
            if (closed_) return false;
            weight_ += w;
            items_.emplace_back(std::move(item), w);
        }
        not_empty_.notify_one();
        return true;
    }

    // nullopt on timeout, or once the queue is closed and drained
    std::optional<T> pop_wait(std::chrono::milliseconds timeout) {
        std::optional<T> out;
        {
            std::unique_lock lock(mutex_);
            if (!not_empty_.wait_for(lock, timeout, [&] { return closed_ || !items_.empty(); })) {
                return std::nullopt;
            }
            if (items_.empty()) return std::nullopt;
            auto& [item, w] = items_.front();
            out.emplace(std::move(item));
            weight_ -= w;
            items_.pop_front();
        }
        not_full_.notify_all();
        return out;
    }

    // Removes every queued item matching pred; returns how many were removed
    template <typename Pred>
    std::size_t remove_if(Pred pred) {
        std::size_t removed = 0;
        {
            std::lock_guard lock(mutex_);
            for (auto it = items_.begin(); it != items_.end();) {
                if (pred(std::as_const(it->first))) {
                    weight_ -= it->second;
                    it = items_.erase(it);
                    ++removed;
                } else {
                    ++it;
                }
            }
        }
        if (removed > 0) not_full_.notify_all();
        return removed;
    }

    // Wakes every waiter; pushes fail afterwards, pops drain what is left
    void close() {
        {
            std::lock_guard lock(mutex_);
            closed_ = true;
        }
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    bool closed() const {
        std::lock_guard lock(mutex_);
        return closed_;
    }

    std::size_t weight() const {
        std::lock_guard lock(mutex_);
        return weight_;
    }

    std::size_t size() const {
        std::lock_guard lock(mutex_);
        return items_.size();
    }

    std::size_t capacity() const { return capacity_; }

private:
    bool fits(std::size_t w) const { return items_.empty() || weight_ + w <= capacity_; }

    const std::size_t capacity_;
    Weigh weigh_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::deque<std::pair<T, std::size_t>> items_;
    std::size_t weight_ = 0;
    bool closed_ = false;
};

}  // namespace appender

#endif  // APPENDER_BOUNDED_QUEUE_HPP
