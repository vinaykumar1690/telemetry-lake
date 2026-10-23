#ifndef OBSERVABILITY_CROW_LOG_HPP
#define OBSERVABILITY_CROW_LOG_HPP

#include "observability/log.hpp"

#include "crow/logging.h"

// Sends Crow's own log lines through the structured logger (header-only so
// the observability library does not depend on Crow)

namespace obs {

class CrowLogHandler : public crow::ILogHandler {
public:
    void log(const std::string& message, crow::LogLevel level) override {
        obs::log(toLevel(level), "crow", message);
    }

    // Installs a process-wide handler; Crow keeps the pointer
    static void install() {
        static CrowLogHandler handler;
        crow::logger::setHandler(&handler);
    }

private:
    static Level toLevel(crow::LogLevel level) {
        switch (level) {
            case crow::LogLevel::Debug: return Level::Debug;
            case crow::LogLevel::Info: return Level::Info;
            case crow::LogLevel::Warning: return Level::Warn;
            default: return Level::Error;
        }
    }
};

}  // namespace obs

#endif  // OBSERVABILITY_CROW_LOG_HPP
