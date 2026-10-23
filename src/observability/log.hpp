#ifndef OBSERVABILITY_LOG_HPP
#define OBSERVABILITY_LOG_HPP

#include <concepts>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

// Structured logging for the services: one line per event on stdout, as JSON
// (for the log collector) or as text (for a terminal)

namespace obs {

enum class Level { Debug, Info, Warn, Error };
enum class LogFormat { Json, Text };

// A field value; integers stay exact, everything else is formatted as is
class FieldValue {
public:
    using Storage = std::variant<std::int64_t, std::uint64_t, double, bool, std::string>;

    template <std::signed_integral T>
    FieldValue(T v) : value_(static_cast<std::int64_t>(v)) {}
    template <std::unsigned_integral T>
        requires(!std::same_as<T, bool>)
    FieldValue(T v) : value_(static_cast<std::uint64_t>(v)) {}
    FieldValue(bool v) : value_(v) {}
    FieldValue(double v) : value_(v) {}
    FieldValue(std::string v) : value_(std::move(v)) {}
    FieldValue(std::string_view v) : value_(std::string(v)) {}
    FieldValue(const char* v) : value_(std::string(v)) {}

    const Storage& value() const { return value_; }

private:
    Storage value_;
};

using Fields = std::initializer_list<std::pair<std::string_view, FieldValue>>;

struct LogConfig {
    std::string service = "telemetry-lake";
    Level level = Level::Info;
    LogFormat format = LogFormat::Text;
};

// Reads LOG_LEVEL (debug|info|warn|error, default info) and LOG_FORMAT
// (json|text; default text on a terminal, json otherwise). Throws on an
// invalid value. Call before starting threads.
void initLogging(std::string service);

void configureLogging(LogConfig config);
const LogConfig& loggingConfig();

// Replaces stdout (tests); an empty function restores it
void setLogSinkForTesting(std::function<void(std::string_view)> sink);

bool enabled(Level level);
std::string_view toString(Level level);

// Writes one line; lines from different threads never interleave
void log(Level level, std::string_view component, std::string_view message, Fields fields = {});

inline void debug(std::string_view component, std::string_view message, Fields fields = {}) {
    if (enabled(Level::Debug)) log(Level::Debug, component, message, fields);
}
inline void info(std::string_view component, std::string_view message, Fields fields = {}) {
    log(Level::Info, component, message, fields);
}
inline void warn(std::string_view component, std::string_view message, Fields fields = {}) {
    log(Level::Warn, component, message, fields);
}
inline void error(std::string_view component, std::string_view message, Fields fields = {}) {
    log(Level::Error, component, message, fields);
}

// Formats one line without the trailing newline (exposed for tests)
std::string formatLine(const LogConfig& config, Level level, std::string_view component,
                       std::string_view message, Fields fields, std::string_view timestamp);

}  // namespace obs

#endif  // OBSERVABILITY_LOG_HPP
