#ifndef APPENDER_ERROR_HPP
#define APPENDER_ERROR_HPP

#include <expected>
#include <string>
#include <string_view>
#include <utility>

namespace appender {

// How the caller must react to a failure
enum class ErrorKind {
    Retryable,      // transient; retry with backoff
    Indeterminate,  // may or may not have taken effect (e.g. a commit timed out)
    Poison,         // the input itself is bad; route it to the DLQ and move on
    Fatal,          // the process cannot continue safely
};

constexpr std::string_view toString(ErrorKind kind) {
    switch (kind) {
        case ErrorKind::Retryable: return "retryable";
        case ErrorKind::Indeterminate: return "indeterminate";
        case ErrorKind::Poison: return "poison";
        case ErrorKind::Fatal: return "fatal";
    }
    return "unknown";
}

struct AppenderError {
    ErrorKind kind;
    std::string message;
};

template <typename T = void>
using Result = std::expected<T, AppenderError>;

inline std::unexpected<AppenderError> retryable(std::string message) {
    return std::unexpected(AppenderError{ErrorKind::Retryable, std::move(message)});
}

inline std::unexpected<AppenderError> indeterminate(std::string message) {
    return std::unexpected(AppenderError{ErrorKind::Indeterminate, std::move(message)});
}

inline std::unexpected<AppenderError> poison(std::string message) {
    return std::unexpected(AppenderError{ErrorKind::Poison, std::move(message)});
}

inline std::unexpected<AppenderError> fatal(std::string message) {
    return std::unexpected(AppenderError{ErrorKind::Fatal, std::move(message)});
}

}  // namespace appender

#endif  // APPENDER_ERROR_HPP
