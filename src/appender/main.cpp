#include "app.hpp"
#include "appender_config.hpp"

#include "observability/crow_log.hpp"
#include "observability/log.hpp"

#include <csignal>
#include <cstdio>
#include <iostream>
#include <pthread.h>

int main() {
    // Block the shutdown/flush signals before any thread exists, so every
    // thread inherits the mask and only the app's sigwait thread sees them
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    sigaddset(&signals, SIGUSR1);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);

    // Line-buffer stdout even when it is a pipe or file (containers), so log
    // lines are not lost on a crash
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    try {
        obs::initLogging("otel-appender");
    } catch (const std::exception& e) {
        std::cerr << "Configuration error: " << e.what() << std::endl;
        return appender::kExitStartup;
    }
    obs::CrowLogHandler::install();

    appender::AppenderConfig config;
    try {
        config = appender::AppenderConfig::fromEnv();
    } catch (const std::exception& e) {
        obs::error("main", "invalid configuration", {{"error", e.what()}});
        obs::error("main", "required: KAFKA_BROKERS, ICEBERG_CATALOG_URI, S3_ENDPOINT, S3_ACCESS_KEY, "
                           "S3_SECRET_KEY; see README.md for the optional settings");
        return appender::kExitStartup;
    }

    try {
        return appender::runAppender(config);
    } catch (const std::exception& e) {
        obs::error("main", "fatal error", {{"error", e.what()}});
        return appender::kExitFatal;
    }
}
