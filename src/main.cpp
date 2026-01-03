#include "ingester/http_server.hpp"
#include "ingester/queue_producer.hpp"
#include "config.hpp"
#include <iostream>
#include <memory>

int main() {
    try {
        // Load configuration from environment
        IngesterConfig config = IngesterConfig::fromEnv();
        
        // Create and initialize queue producer
        auto queue_producer = std::make_shared<QueueProducer>(config);
        if (!queue_producer->initialize()) {
            std::cerr << "Warning: Failed to initialize queue producer. Continuing without queue support." << std::endl;
            queue_producer.reset();
        }
        
        // Create HTTP server with queue producer
        HttpServer server(queue_producer);
        server.start("0.0.0.0", 4318);
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        std::cerr << "Please set required environment variables:" << std::endl;
        std::cerr << "  REDPANDA_BROKERS - Comma-separated list of broker addresses" << std::endl;
        std::cerr << "  REDPANDA_TOPIC - Topic name (optional, defaults to 'otel-logs')" << std::endl;
        return 1;
    }
    return 0;
}
