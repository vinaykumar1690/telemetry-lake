#include "health_server.hpp"

#include "observability/log.hpp"

#include <format>

namespace appender {

HealthServer::HealthServer(std::uint16_t port, Handlers handlers)
    : port_(port), handlers_(std::move(handlers)) {
    CROW_ROUTE(app_, "/health")([this] {
        // Liveness: only a fatal error warrants a restart
        return handlers_.snapshot().fatal ? crow::response(503, "fatal")
                                          : crow::response(200, "OK");
    });

    CROW_ROUTE(app_, "/ready")([this] {
        const auto readiness = evaluateReadiness(handlers_.snapshot());
        return crow::response(readiness.ready ? 200 : 503, readiness.reason);
    });

    CROW_ROUTE(app_, "/flush").methods(crow::HTTPMethod::Post)([this] {
        auto result = handlers_.flush();
        if (!result) {
            return crow::response(503, std::format("flush failed ({}): {}",
                                                   toString(result.error().kind),
                                                   result.error().message));
        }
        return crow::response(200, "flushed: staged data is committed to Iceberg");
    });

    CROW_ROUTE(app_, "/metrics")([this] {
        crow::response response(200, handlers_.metrics ? handlers_.metrics()
                                                        : formatMetrics(handlers_.snapshot()));
        response.set_header("Content-Type", "text/plain; version=0.0.4");
        return response;
    });
}

HealthServer::~HealthServer() {
    stop();
}

void HealthServer::start() {
    // Signals are handled by the app's sigwait thread, not by Crow
    app_.signal_clear();
    app_.loglevel(crow::LogLevel::Warning);
    running_ = app_.port(port_).concurrency(2).run_async();
    obs::info("health", "health server listening",
              {{"port", port_}, {"paths", "/health /ready /flush /metrics"}});
}

void HealthServer::stop() {
    if (!running_.valid()) return;
    app_.stop();
    running_.wait();
}

}  // namespace appender
