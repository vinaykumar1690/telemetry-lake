#include "partition_coordinator.hpp"
#include "dead_letter_queue.hpp"
#include "../config.hpp"
#include "crow.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>
#include <cstdlib>

std::atomic<bool> g_running(true);
std::atomic<bool> g_force_flush(false);
PartitionCoordinator* g_coordinator = nullptr;

void signalHandler(int signal) {
    if (signal == SIGUSR1) {
        std::cout << "\nReceived SIGUSR1, forcing flush..." << std::endl;
        g_force_flush = true;
    } else {
        std::cout << "\nReceived signal " << signal << ", shutting down gracefully..." << std::endl;
        g_running = false;
        if (g_coordinator) {
            g_coordinator->stop();
        }
    }
}

void runHealthServer(int port, PartitionCoordinator* coordinator) {
    crow::SimpleApp app;

    // Health check endpoint
    CROW_ROUTE(app, "/health")
    ([]() {
        return crow::response(200, "OK");
    });

    // Readiness check
    CROW_ROUTE(app, "/ready")
    ([]() {
        return crow::response(200, "OK");
    });

    // Force flush endpoint
    CROW_ROUTE(app, "/flush").methods("POST"_method)
    ([coordinator]() {
        std::cout << "Force flush requested via HTTP endpoint" << std::endl;
        if (coordinator->forceFlushAll()) {
            return crow::response(200, "Flush completed successfully (offsets committed)");
        } else {
            return crow::response(500, "Flush failed (some partitions may not have flushed)");
        }
    });

    // Buffer stats endpoint
    CROW_ROUTE(app, "/stats")
    ([coordinator]() {
        crow::json::wvalue stats;
        stats["total_buffer_size_bytes"] = coordinator->getTotalBufferSize();
        stats["total_buffer_records"] = coordinator->getTotalBufferRecordCount();
        stats["is_running"] = coordinator->isRunning();
        return crow::response(200, stats);
    });

    std::cout << "Appender health server running on port " << port << std::endl;
    std::cout << "  POST /flush - Force flush all partitions to Iceberg" << std::endl;
    std::cout << "  GET /stats - Get aggregate buffer statistics" << std::endl;
    std::cout << "  GET /health - Health check" << std::endl;

    app.port(port).multithreaded().run();
}

int main() {
    try {
        // Set up signal handlers
        std::signal(SIGINT, signalHandler);
        std::signal(SIGTERM, signalHandler);
        std::signal(SIGUSR1, signalHandler);  // Force flush signal

        // Load configuration from environment
        AppenderConfig config = AppenderConfig::fromEnv();

        // Get health server port from environment (default: 8080)
        int health_port = 8080;
        const char* health_port_str = std::getenv("HEALTH_PORT");
        if (health_port_str) {
            health_port = std::atoi(health_port_str);
        }

        // Initialize partition coordinator
        PartitionCoordinator coordinator(config);
        g_coordinator = &coordinator;

        if (!coordinator.initialize()) {
            std::cerr << "Failed to initialize partition coordinator" << std::endl;
            return 1;
        }

        // Initialize dead letter queue (optional)
        const char* dlq_path = std::getenv("DLQ_PATH");
        DeadLetterQueue dlq(dlq_path ? dlq_path : "");

        // Start health server in a separate thread
        std::thread health_thread(runHealthServer, health_port, &coordinator);
        health_thread.detach();

        std::cout << "OTel Log Appender started successfully (multi-partition mode)" << std::endl;
        std::cout << "Per-partition buffer settings: " << config.partition_buffer_size_mb << " MB or "
                  << config.partition_buffer_time_seconds << " seconds" << std::endl;
        std::cout << "Exactly-once semantics enabled: offsets committed after Iceberg flush" << std::endl;
        std::cout << "Iceberg commit retries: " << config.iceberg_commit_retries
                  << " (base delay: " << config.iceberg_retry_base_delay_ms << "ms)" << std::endl;
        std::cout << "Send SIGUSR1 to force flush all partitions (kill -USR1 <pid>)" << std::endl;

        // Monitor for force flush signal in a separate check
        std::thread flush_monitor([&coordinator]() {
            while (g_running) {
                if (g_force_flush.exchange(false)) {
                    std::cout << "Processing force flush request..." << std::endl;
                    if (coordinator.forceFlushAll()) {
                        std::cout << "Force flush completed" << std::endl;
                    } else {
                        std::cerr << "Force flush failed for some partitions" << std::endl;
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        });
        flush_monitor.detach();

        // Start the coordinator (this blocks until stopped)
        coordinator.start();

        // If we get here, the coordinator has stopped
        g_coordinator = nullptr;
        std::cout << "Appender stopped" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        std::cerr << "Please set required environment variables:" << std::endl;
        std::cerr << "  KAFKA_BROKERS - Comma-separated list of broker addresses" << std::endl;
        std::cerr << "  KAFKA_TOPIC - Topic name (optional, defaults to 'otel-logs')" << std::endl;
        std::cerr << "  KAFKA_CONSUMER_GROUP - Consumer group name (optional, defaults to 'otel-appender')" << std::endl;
        std::cerr << "  ICEBERG_CATALOG_URI - Iceberg REST catalog URI" << std::endl;
        std::cerr << "  S3_ENDPOINT - S3-compatible storage endpoint" << std::endl;
        std::cerr << "  S3_ACCESS_KEY - S3 access key" << std::endl;
        std::cerr << "  S3_SECRET_KEY - S3 secret key" << std::endl;
        std::cerr << "  S3_BUCKET - S3 bucket name" << std::endl;
        std::cerr << "Optional:" << std::endl;
        std::cerr << "  PARTITION_BUFFER_SIZE_MB - Per-partition buffer size threshold (default: 50)" << std::endl;
        std::cerr << "  PARTITION_BUFFER_TIME_SECONDS - Per-partition buffer time threshold (default: 60)" << std::endl;
        std::cerr << "  ICEBERG_COMMIT_RETRIES - Max Iceberg commit retries (default: 5)" << std::endl;
        std::cerr << "  ICEBERG_RETRY_BASE_DELAY_MS - Base retry delay in ms (default: 100)" << std::endl;
        std::cerr << "  ICEBERG_RETRY_MAX_DELAY_MS - Max retry delay in ms (default: 5000)" << std::endl;
        std::cerr << "  REBALANCE_TIMEOUT_SECONDS - Worker shutdown timeout on rebalance (default: 30)" << std::endl;
        std::cerr << "  HEALTH_PORT - Health/flush endpoint port (default: 8080)" << std::endl;
        return 1;
    }

    return 0;
}
