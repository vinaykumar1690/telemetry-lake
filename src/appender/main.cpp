#include "app.hpp"
#include "appender_config.hpp"

#include <csignal>
#include <format>
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

    appender::AppenderConfig config;
    try {
        config = appender::AppenderConfig::fromEnv();
    } catch (const std::exception& e) {
        std::cerr << std::format("Configuration error: {}\n", e.what());
        std::cerr << "Required: KAFKA_BROKERS, ICEBERG_CATALOG_URI, S3_ENDPOINT, S3_ACCESS_KEY, "
                     "S3_SECRET_KEY. See CLAUDE.md for the optional settings.\n";
        return appender::kExitStartup;
    }

    try {
        return appender::runAppender(config);
    } catch (const std::exception& e) {
        std::cerr << std::format("Fatal: {}\n", e.what());
        return appender::kExitFatal;
    }
}
