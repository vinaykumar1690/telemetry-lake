#ifndef HTTP_SERVER_HPP
#define HTTP_SERVER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include "crow.h"

namespace prometheus {
class Registry;
}

class QueueProducer;
struct IngesterMetrics;

struct HttpServerOptions {
    std::string host = "0.0.0.0";
    std::uint16_t port = 4318;
    // Worker threads; 0 means one per hardware thread
    unsigned threads = 0;
    // Limit on the request body as received (before decompression). Crow buffers
    // the whole body before routing, so also cap it at the ingress/proxy.
    std::size_t max_body_bytes = 16 * 1024 * 1024;
    // Limit on the payload after gzip decompression (decompression-bomb guard).
    // The payload becomes one Kafka message, so keep this within message.max.bytes.
    std::size_t max_decompressed_bytes = 16 * 1024 * 1024;
    std::string server_name = "telemetry-lake-ingester";
    crow::LogLevel log_level = crow::LogLevel::Warning;
};

class HttpServer {
public:
    // Without a producer, accepted payloads are discarded and /ready reports 503
    // (used by tests). Without metrics the server keeps its own, unexported.
    HttpServer();
    explicit HttpServer(std::shared_ptr<QueueProducer> queue_producer,
                        HttpServerOptions options = {},
                        std::shared_ptr<IngesterMetrics> metrics = nullptr);
    ~HttpServer();

    // Bind to options.host:options.port and serve until SIGINT/SIGTERM
    void start();

    // Setup routes on the provided app (for testing)
    void setupRoutes(crow::SimpleApp& app);

    IngesterMetrics& metrics() { return *metrics_; }

private:
    std::shared_ptr<QueueProducer> queue_producer_;
    HttpServerOptions options_;
    std::shared_ptr<prometheus::Registry> own_registry_;  // when no metrics were passed
    std::shared_ptr<IngesterMetrics> metrics_;
};

#endif // HTTP_SERVER_HPP
