#include "buffer_manager.hpp"
#include <algorithm>

BufferManager::BufferManager(size_t max_size_bytes, int max_time_seconds)
    : max_size_bytes_(max_size_bytes),
      max_time_seconds_(max_time_seconds),
      current_size_(0),
      last_reset_time_(std::chrono::system_clock::now()) {
}

bool BufferManager::add(size_t data_size_bytes) {
    size_t new_size = current_size_.fetch_add(data_size_bytes) + data_size_bytes;
    return new_size >= max_size_bytes_;
}

bool BufferManager::shouldFlushByTime() const {
    std::lock_guard<std::mutex> lock(time_mutex_);
    auto now = std::chrono::system_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_reset_time_);
    return elapsed.count() >= max_time_seconds_;
}

void BufferManager::resetTime() {
    std::lock_guard<std::mutex> lock(time_mutex_);
    last_reset_time_ = std::chrono::system_clock::now();
}

std::chrono::seconds BufferManager::getTimeSinceReset() const {
    std::lock_guard<std::mutex> lock(time_mutex_);
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::seconds>(now - last_reset_time_);
}

void BufferManager::reset() {
    current_size_.store(0);
    resetTime();
}

