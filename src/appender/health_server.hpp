#ifndef APPENDER_HEALTH_SERVER_HPP
#define APPENDER_HEALTH_SERVER_HPP

#include "error.hpp"
#include "health.hpp"

#include "crow.h"

#include <cstdint>
#include <functional>
#include <future>

namespace appender {

// /health (liveness), /ready, POST /flush and /metrics on a small Crow
// server. Owned by the app and stopped before anything it reads is destroyed.
class HealthServer {
public:
    struct Handlers {
        std::function<HealthSnapshot()> snapshot;
        std::function<Result<void>()> flush;  // blocks until the flush cycle ends
    };

    HealthServer(std::uint16_t port, Handlers handlers);
    ~HealthServer();

    void start();
    void stop();

private:
    std::uint16_t port_;
    Handlers handlers_;
    crow::SimpleApp app_;
    std::future<void> running_;
};

}  // namespace appender

#endif  // APPENDER_HEALTH_SERVER_HPP
