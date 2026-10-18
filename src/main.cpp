#include "ingester/http_server.hpp"
#include "ingester/queue_producer.hpp"
#include "config.hpp"
#include <iostream>
#include <memory>

int main() {
    IngesterConfig config;
    try {
        // Load configuration from environment
        config = IngesterConfig::fromEnv();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        std::cerr << "Please set required environment variables:" << std::endl;
        std::cerr << "  KAFKA_BROKERS - Comma-separated list of broker addresses" << std::endl;
        std::cerr << "  KAFKA_TOPIC - Topic name (optional, defaults to 'otel-logs')" << std::endl;
        return 1;
    }

    try {
        // Without a producer every accepted request would be dropped, so exit and
        // let the orchestrator restart us rather than serving traffic.
        auto queue_producer = std::make_shared<QueueProducer>(config);
        if (!queue_producer->initialize()) {
            std::cerr << "Fatal error: failed to initialize queue producer" << std::endl;
            return 1;
        }

        HttpServerOptions options;
        options.host = config.http_host;
        options.port = config.http_port;
        options.threads = config.http_threads;
        options.max_body_bytes = config.max_body_bytes;
        options.max_decompressed_bytes = config.max_decompressed_bytes;

        HttpServer server(queue_producer, std::move(options));
        server.start();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
