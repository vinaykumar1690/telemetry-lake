#ifndef BUFFER_MANAGER_HPP
#define BUFFER_MANAGER_HPP

#include <chrono>
#include <mutex>
#include <atomic>

class BufferManager {
public:
    BufferManager(size_t max_size_bytes, int max_time_seconds);
    
    // Add data to buffer, returns true if flush should be triggered
    bool add(size_t data_size_bytes);
    
    // Check if time threshold is met
    bool shouldFlushByTime() const;
    
    // Reset time counter (call after flush)
    void resetTime();
    
    // Get current buffer size
    size_t getCurrentSize() const { return current_size_.load(); }
    
    // Get time since last reset
    std::chrono::seconds getTimeSinceReset() const;
    
    // Reset buffer (call after flush)
    void reset();

private:
    size_t max_size_bytes_;
    int max_time_seconds_;
    std::atomic<size_t> current_size_;
    std::chrono::system_clock::time_point last_reset_time_;
    mutable std::mutex time_mutex_;
};

#endif // BUFFER_MANAGER_HPP

