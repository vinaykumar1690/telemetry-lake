#include "observability/log.hpp"

#include <nlohmann/json.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <format>
#include <iterator>
#include <mutex>
#include <stdexcept>
#include <unistd.h>

namespace obs {
namespace {

LogConfig g_config;
std::atomic<int> g_level{static_cast<int>(Level::Info)};
std::mutex g_write_mutex;
std::function<void(std::string_view)> g_test_sink;  // guarded by g_write_mutex

std::string timestampNow() {
    return std::format("{:%FT%T}Z", std::chrono::floor<std::chrono::milliseconds>(
                                         std::chrono::system_clock::now()));
}

void writeAll(int fd, std::string_view data) {
    while (!data.empty()) {
        const auto n = ::write(fd, data.data(), data.size());
        if (n < 0) {
            if (errno == EINTR) continue;
            return;  // nowhere left to report it
        }
        data.remove_prefix(static_cast<std::size_t>(n));
    }
}

nlohmann::ordered_json toJson(const FieldValue& value) {
    return std::visit([](const auto& v) { return nlohmann::ordered_json(v); }, value.value());
}

// Text values are quoted when they would not read back as one token
void appendText(std::string& out, const FieldValue& value) {
    std::visit(
        [&](const auto& v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::string>) {
                const bool plain = !v.empty() && v.find_first_of(" \t\n\"=") == std::string::npos;
                if (plain) out += v;
                else out += nlohmann::json(v).dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
            } else if constexpr (std::is_same_v<T, bool>) {
                out += v ? "true" : "false";
            } else {
                std::format_to(std::back_inserter(out), "{}", v);
            }
        },
        value.value());
}

Level parseLevel(std::string_view text) {
    if (text == "debug") return Level::Debug;
    if (text == "info") return Level::Info;
    if (text == "warn" || text == "warning") return Level::Warn;
    if (text == "error") return Level::Error;
    throw std::runtime_error(
        std::format("LOG_LEVEL must be one of debug, info, warn, error; got '{}'", text));
}

LogFormat parseFormat(std::string_view text) {
    if (text == "json") return LogFormat::Json;
    if (text == "text") return LogFormat::Text;
    throw std::runtime_error(std::format("LOG_FORMAT must be json or text; got '{}'", text));
}

}  // namespace

std::string_view toString(Level level) {
    switch (level) {
        case Level::Debug: return "debug";
        case Level::Info: return "info";
        case Level::Warn: return "warn";
        case Level::Error: return "error";
    }
    return "info";
}

void initLogging(std::string service) {
    LogConfig config;
    config.service = std::move(service);
    config.format = ::isatty(STDOUT_FILENO) ? LogFormat::Text : LogFormat::Json;
    if (const char* level = std::getenv("LOG_LEVEL"); level && *level) {
        config.level = parseLevel(level);
    }
    if (const char* format = std::getenv("LOG_FORMAT"); format && *format) {
        config.format = parseFormat(format);
    }
    configureLogging(std::move(config));
}

void configureLogging(LogConfig config) {
    std::lock_guard lock(g_write_mutex);
    g_level = static_cast<int>(config.level);
    g_config = std::move(config);
}

const LogConfig& loggingConfig() {
    return g_config;
}

void setLogSinkForTesting(std::function<void(std::string_view)> sink) {
    std::lock_guard lock(g_write_mutex);
    g_test_sink = std::move(sink);
}

bool enabled(Level level) {
    return static_cast<int>(level) >= g_level.load(std::memory_order_relaxed);
}

std::string formatLine(const LogConfig& config, Level level, std::string_view component,
                       std::string_view message, Fields fields, std::string_view timestamp) {
    if (config.format == LogFormat::Json) {
        nlohmann::ordered_json line;
        line["ts"] = timestamp;
        line["level"] = toString(level);
        line["service"] = config.service;
        line["component"] = component;
        line["msg"] = message;
        for (const auto& [key, value] : fields) line[std::string(key)] = toJson(value);
        // Error messages may carry arbitrary bytes; never fail on them
        return line.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
    }

    std::string out;
    std::format_to(std::back_inserter(out), "{} {:<5} {}: {}", timestamp, toString(level),
                   component, message);
    for (const auto& [key, value] : fields) {
        std::format_to(std::back_inserter(out), " {}=", key);
        appendText(out, value);
    }
    return out;
}

void log(Level level, std::string_view component, std::string_view message, Fields fields) {
    if (!enabled(level)) return;
    const auto timestamp = timestampNow();
    std::lock_guard lock(g_write_mutex);
    std::string line = formatLine(g_config, level, component, message, fields, timestamp);
    line += '\n';
    if (g_test_sink) g_test_sink(line);
    else writeAll(STDOUT_FILENO, line);
}

}  // namespace obs
