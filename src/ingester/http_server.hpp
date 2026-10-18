#ifndef HTTP_SERVER_HPP
#define HTTP_SERVER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include "crow.h"

class QueueProducer;

struct HttpServerOptions {
    std::string host = "0.0.0.0";
    std::uint16_t port = 4318;
    // Worker threads; 0 means one per hardware thread
    unsigned threads = 0;
    // Limit on the request body as received (before decompression). Crow buffers
    // the whole body before routing, so also cap it at the ingress/proxy.
    std::size_t max_body_bytes = 16 * 1024 * 1024;
    // Limit on the payload after gzip decompression (decompression-bomb guard)
    std::size_t max_decompressed_bytes = 64 * 1024 * 1024;
    std::string server_name = "telemetry-lake-ingester";
    crow::LogLevel log_level = crow::LogLevel::Warning;
};

class HttpServer {
public:
    // Without a producer, accepted payloads are discarded and /ready reports 503
    // (used by tests)
    HttpServer();
    explicit HttpServer(std::shared_ptr<QueueProducer> queue_producer,
                        HttpServerOptions options = {});

    // Bind to options.host:options.port and serve until SIGINT/SIGTERM
    void start();

    // Setup routes on the provided app (for testing)
    void setupRoutes(crow::SimpleApp& app);

private:
    std::shared_ptr<QueueProducer> queue_producer_;
    HttpServerOptions options_;
};

#endif // HTTP_SERVER_HPP
