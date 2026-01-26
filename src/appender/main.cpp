#include "queue_consumer.hpp"
#include "log_transformer.hpp"
#include "iceberg_appender.hpp"
#include "buffer_manager.hpp"
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

void signalHandler(int signal) {
    if (signal == SIGUSR1) {
        std::cout << "\nReceived SIGUSR1, forcing flush..." << std::endl;
        g_force_flush = true;
    } else {
        std::cout << "\nReceived signal " << signal << ", shutting down gracefully..." << std::endl;
        g_running = false;
    }
}

void runHealthServer(int port, IcebergAppender* appender, BufferManager* buffer_manager) {
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
    ([appender, buffer_manager]() {
        std::cout << "Force flush requested via HTTP endpoint" << std::endl;
        if (appender->flush()) {
            buffer_manager->reset();
            return crow::response(200, "Flush completed successfully");
        } else {
            return crow::response(500, "Flush failed");
        }
    });

    // Buffer stats endpoint
    CROW_ROUTE(app, "/stats")
    ([appender, buffer_manager]() {
        crow::json::wvalue stats;
        stats["buffer_size_bytes"] = appender->getBufferSize();
        stats["buffer_records"] = appender->getBufferRecordCount();
        stats["time_since_last_flush_seconds"] = buffer_manager->getTimeSinceReset().count();
        return crow::response(200, stats);
    });

    std::cout << "Appender health server running on port " << port << std::endl;
    std::cout << "  POST /flush - Force flush buffered data to Iceberg" << std::endl;
    std::cout << "  GET /stats - Get buffer statistics" << std::endl;
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

        // Create buffer manager
        size_t buffer_size_bytes = config.buffer_size_mb * 1024 * 1024;
        BufferManager buffer_manager(buffer_size_bytes, config.buffer_time_seconds);

        // Initialize queue consumer
        QueueConsumer consumer(config);
        if (!consumer.initialize()) {
            std::cerr << "Failed to initialize queue consumer" << std::endl;
            return 1;
        }

        // Initialize Iceberg appender
        IcebergAppender appender(config);
        if (!appender.initialize()) {
            std::cerr << "Failed to initialize Iceberg appender" << std::endl;
            return 1;
        }

        // Initialize dead letter queue (optional)
        const char* dlq_path = std::getenv("DLQ_PATH");
        DeadLetterQueue dlq(dlq_path ? dlq_path : "");

        // Start health server in a separate thread
        std::thread health_thread(runHealthServer, health_port, &appender, &buffer_manager);
        health_thread.detach();

        std::cout << "OTel Log Appender started successfully" << std::endl;
        std::cout << "Buffer settings: " << config.buffer_size_mb << " MB or "
                  << config.buffer_time_seconds << " seconds" << std::endl;
        std::cout << "Send SIGUSR1 to force flush (kill -USR1 <pid>)" << std::endl;

        // Start consuming messages
        consumer.start([&](const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request) {
            if (!g_running) {
                return; // Stop processing if shutdown requested
            }

            // Check for force flush signal
            if (g_force_flush.exchange(false)) {
                std::cout << "Processing force flush request..." << std::endl;
                if (appender.flush()) {
                    buffer_manager.reset();
                    std::cout << "Force flush completed" << std::endl;
                } else {
                    std::cerr << "Force flush failed" << std::endl;
                }
            }

            try {
                // Transform log records
                auto transformed = LogTransformer::transform(request);

                if (transformed.empty()) {
                    return; // No records to process
                }

                // Estimate data size for buffer manager
                size_t estimated_size = 0;
                for (const auto& record : transformed) {
                    estimated_size += record.body.size() + record.severity.size() +
                                    record.service_name.size() + record.deployment_environment.size() +
                                    record.host_name.size() + record.trace_id.size() + record.span_id.size();
                    for (const auto& attr : record.attributes) {
                        estimated_size += attr.first.size() + attr.second.size();
                    }
                    estimated_size += 100; // overhead
                }

                // Add to buffer manager and check if size threshold is met
                bool size_threshold_met = buffer_manager.add(estimated_size);

                // Append to Iceberg appender
                bool flush_triggered = appender.append(transformed);

                // Check time threshold
                bool time_threshold_met = buffer_manager.shouldFlushByTime();

                // Flush if either threshold is met
                if (size_threshold_met || time_threshold_met || flush_triggered) {
                    std::cout << "Flush triggered: size=" << size_threshold_met
                              << ", time=" << time_threshold_met << std::endl;
                    if (appender.flush()) {
                        buffer_manager.reset();
                    } else {
                        std::cerr << "Flush failed, keeping data in buffer" << std::endl;
                    }
                }
            } catch (const std::exception& e) {
                std::cerr << "Error processing message: " << e.what() << std::endl;

                // Write to dead letter queue if enabled
                if (dlq.isEnabled()) {
                    dlq.write(request, std::string("Processing error: ") + e.what());
                }

                // Continue processing - don't crash on individual message errors
            }
        });

        // If we get here, the consumer has stopped
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
        std::cerr << "  BUFFER_SIZE_MB - Buffer size threshold (default: 100)" << std::endl;
        std::cerr << "  BUFFER_TIME_SECONDS - Buffer time threshold (default: 300)" << std::endl;
        std::cerr << "  HEALTH_PORT - Health/flush endpoint port (default: 8080)" << std::endl;
        return 1;
    }

    return 0;
}
