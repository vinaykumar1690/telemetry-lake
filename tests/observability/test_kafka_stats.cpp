#include "observability/kafka_stats.hpp"
#include "observability/metrics.hpp"

#include <gtest/gtest.h>
#include <prometheus/text_serializer.h>

#include <fstream>
#include <set>
#include <sstream>

namespace {

// Captured from librdkafka 2.12 against a one-broker cluster: a producer that
// sent 50 messages, and a consumer assigned partitions 0-3 of e2e-logs
std::string fixture(const char* name) {
    std::ifstream in(std::string(OBS_TEST_DATA_DIR) + "/" + name);
    std::stringstream buffer;
    buffer << in.rdbuf();
    return buffer.str();
}

std::string render(const obs::KafkaStatsCollector& collector) {
    return prometheus::TextSerializer().Serialize(collector.Collect());
}

TEST(KafkaStats, ConsumerLagIsPositionBased) {
    obs::KafkaStatsCollector collector;
    ASSERT_TRUE(collector.update("appender-consumer", fixture("consumer_stats.json")));
    const auto text = render(collector);

    // Position 45 at high watermark 45: no lag, although librdkafka's own
    // consumer_lag is -1 because nothing was committed
    EXPECT_NE(text.find(R"(rdkafka_consumer_lag_messages{client="appender-consumer",topic="e2e-logs",partition="0"} 0)"),
              std::string::npos) << text;
    // Partition 2 has no position yet, so no lag sample
    EXPECT_EQ(text.find(R"(rdkafka_consumer_lag_messages{client="appender-consumer",topic="e2e-logs",partition="2"})"),
              std::string::npos);
    EXPECT_NE(text.find(R"(rdkafka_partition_high_watermark{client="appender-consumer",topic="e2e-logs",partition="0"} 45)"),
              std::string::npos);
    // Never committed (-1001): omitted rather than exported as a bogus offset
    EXPECT_EQ(text.find("rdkafka_consumer_committed_offset"), std::string::npos);
    // Internal partition -1 and the logical GroupCoordinator are skipped
    EXPECT_EQ(text.find(R"(partition="-1")"), std::string::npos);
    EXPECT_EQ(text.find("GroupCoordinator"), std::string::npos);
    EXPECT_NE(text.find(R"(rdkafka_cgrp_up{client="appender-consumer"} 1)"), std::string::npos);
    EXPECT_NE(text.find(R"(rdkafka_cgrp_assigned_partitions{client="appender-consumer"} 4)"), std::string::npos);
    EXPECT_NE(text.find(R"(rdkafka_rx_messages_total{client="appender-consumer"} 50)"), std::string::npos);
    EXPECT_NE(text.find(R"(rdkafka_broker_up{client="appender-consumer",broker="localhost:9092/1"} 1)"),
              std::string::npos);
    // Producer queue metrics belong to producers only
    EXPECT_EQ(text.find("rdkafka_producer_queue_messages"), std::string::npos);
}

TEST(KafkaStats, HighWatermarks) {
    obs::KafkaStatsCollector collector;
    ASSERT_TRUE(collector.update("appender-consumer", fixture("consumer_stats.json")));
    const auto marks = collector.highWatermarks("appender-consumer", "e2e-logs");
    EXPECT_EQ(marks, (std::map<std::int32_t, std::int64_t>{{0, 45}, {1, 3}, {2, 0}, {3, 2}}));
    EXPECT_TRUE(collector.highWatermarks("appender-consumer", "other").empty());
    EXPECT_TRUE(collector.highWatermarks("nobody", "e2e-logs").empty());
}

TEST(KafkaStats, ProducerAndConsumerShareFamilies) {
    obs::KafkaStatsCollector collector;
    ASSERT_TRUE(collector.update("ingester", fixture("producer_stats.json")));
    ASSERT_TRUE(collector.update("appender-consumer", fixture("consumer_stats.json")));
    const auto families = collector.Collect();

    // Every family name appears once, with samples of both clients
    std::set<std::string> names;
    for (const auto& family : families) EXPECT_TRUE(names.insert(family.name).second) << family.name;

    const auto text = render(collector);
    EXPECT_NE(text.find(R"(rdkafka_tx_messages_total{client="ingester"} 50)"), std::string::npos) << text;
    EXPECT_NE(text.find(R"(rdkafka_producer_queue_messages{client="ingester"})"), std::string::npos);
    EXPECT_NE(text.find(R"(rdkafka_tx_bytes_total{client="appender-consumer"})"), std::string::npos);
    EXPECT_NE(text.find(R"(rdkafka_broker_rtt_avg_seconds{client="ingester",broker="localhost:9092/1"})"),
              std::string::npos);
}

TEST(KafkaStats, InvalidJsonKeepsPrevious) {
    obs::KafkaStatsCollector collector;
    ASSERT_TRUE(collector.update("ingester", fixture("producer_stats.json")));
    EXPECT_FALSE(collector.update("ingester", "{not json"));
    EXPECT_NE(render(collector).find(R"(client="ingester")"), std::string::npos);
}

TEST(KafkaStats, LogLevels) {
    EXPECT_EQ(obs::kafkaLogLevel(3), obs::Level::Error);
    EXPECT_EQ(obs::kafkaLogLevel(4), obs::Level::Warn);
    EXPECT_EQ(obs::kafkaLogLevel(6), obs::Level::Info);
    EXPECT_EQ(obs::kafkaLogLevel(7), obs::Level::Debug);
}

}  // namespace
