#include "observability/log.hpp"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <string>
#include <vector>

namespace {

obs::LogConfig jsonConfig() {
    return {.service = "otel-appender", .level = obs::Level::Info, .format = obs::LogFormat::Json};
}

TEST(Log, JsonLine) {
    const auto line = obs::formatLine(jsonConfig(), obs::Level::Warn, "committer", "commit failed",
                                      {{"partition", 3},
                                       {"offset", std::int64_t{1} << 62},
                                       {"retryable", true},
                                       {"seconds", 1.5},
                                       {"error", "quote \" and \n newline"}},
                                      "2026-10-23T10:00:00.123Z");
    const auto parsed = nlohmann::json::parse(line);
    EXPECT_EQ(parsed["ts"], "2026-10-23T10:00:00.123Z");
    EXPECT_EQ(parsed["level"], "warn");
    EXPECT_EQ(parsed["service"], "otel-appender");
    EXPECT_EQ(parsed["component"], "committer");
    EXPECT_EQ(parsed["msg"], "commit failed");
    EXPECT_EQ(parsed["partition"], 3);
    EXPECT_EQ(parsed["offset"].get<std::int64_t>(), std::int64_t{1} << 62);
    EXPECT_EQ(parsed["retryable"], true);
    EXPECT_EQ(parsed["seconds"], 1.5);
    EXPECT_EQ(parsed["error"], "quote \" and \n newline");
    EXPECT_EQ(line.find('\n'), std::string::npos);
}

TEST(Log, JsonReplacesInvalidUtf8) {
    const auto line = obs::formatLine(jsonConfig(), obs::Level::Error, "sink", "bad \xff bytes", {},
                                      "t");
    EXPECT_NO_THROW((void)nlohmann::json::parse(line));
}

TEST(Log, TextLine) {
    obs::LogConfig config = jsonConfig();
    config.format = obs::LogFormat::Text;
    const auto line = obs::formatLine(config, obs::Level::Info, "kafka", "partitions assigned",
                                      {{"partitions", "0,1"}, {"count", 2}, {"note", "two words"}},
                                      "2026-10-23T10:00:00.123Z");
    EXPECT_EQ(line, R"(2026-10-23T10:00:00.123Z info  kafka: partitions assigned partitions=0,1 count=2 note="two words")");
}

TEST(Log, LevelFilterAndSink) {
    std::vector<std::string> lines;
    obs::configureLogging({.service = "test", .level = obs::Level::Warn, .format = obs::LogFormat::Json});
    obs::setLogSinkForTesting([&](std::string_view line) { lines.emplace_back(line); });
    obs::info("c", "dropped");
    obs::debug("c", "dropped");
    obs::warn("c", "kept", {{"k", "v"}});
    obs::setLogSinkForTesting({});
    obs::configureLogging({});

    ASSERT_EQ(lines.size(), 1u);
    EXPECT_TRUE(lines[0].ends_with("\n"));
    EXPECT_EQ(nlohmann::json::parse(lines[0])["msg"], "kept");
}

}  // namespace
