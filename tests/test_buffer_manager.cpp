#include <gtest/gtest.h>
#include "../src/appender/buffer_manager.hpp"
#include <thread>
#include <chrono>

TEST(BufferManagerTest, SizeThreshold) {
    BufferManager manager(1024, 60); // 1KB or 60 seconds

    // Add data up to threshold (need > 1024 bytes to trigger flush)
    bool should_flush = false;
    for (int i = 0; i < 11; ++i) {
        should_flush = manager.add(100);
        if (should_flush) break;
    }

    EXPECT_TRUE(should_flush);
    EXPECT_GE(manager.getCurrentSize(), 1024u);
}

TEST(BufferManagerTest, TimeThreshold) {
    BufferManager manager(1024 * 1024, 1); // 1MB or 1 second
    
    // Wait a bit more than 1 second
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    
    EXPECT_TRUE(manager.shouldFlushByTime());
}

TEST(BufferManagerTest, Reset) {
    BufferManager manager(1024, 60);
    
    manager.add(500);
    EXPECT_EQ(manager.getCurrentSize(), 500u);
    
    manager.reset();
    EXPECT_EQ(manager.getCurrentSize(), 0u);
    EXPECT_FALSE(manager.shouldFlushByTime());
}

TEST(BufferManagerTest, TimeSinceReset) {
    BufferManager manager(1024, 60);
    
    auto time1 = manager.getTimeSinceReset();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto time2 = manager.getTimeSinceReset();
    
    EXPECT_GE(time2.count(), time1.count());
}

