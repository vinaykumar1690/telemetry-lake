#include "http_server.hpp"
#include "queue_producer.hpp"
#include "crow.h"
#include <iostream>
#include <algorithm>
#include <cctype>
#include <zlib.h>
#include "telemetry_wrapper.pb.h"
#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"

using opentelemetry::proto::collector::logs::v1::ExportLogsServiceResponse;

static bool decompressGzip(const std::string &in, std::string &out) {
    if (in.empty()) { out.clear(); return true; }
    z_stream strm{};
    strm.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(in.data()));
    strm.avail_in = static_cast<uInt>(in.size());

    // 16 + MAX_WBITS to enable gzip decoding with automatic header detection
    if (inflateInit2(&strm, 16 + MAX_WBITS) != Z_OK) {
        return false;
    }

    char buf[4096];
    int ret;
    do {
        strm.next_out = reinterpret_cast<Bytef*>(buf);
        strm.avail_out = sizeof(buf);
        ret = inflate(&strm, Z_NO_FLUSH);
        if (ret != Z_OK && ret != Z_STREAM_END) {
            inflateEnd(&strm);
            return false;
        }
        size_t have = sizeof(buf) - strm.avail_out;
        out.append(buf, have);
    } while (ret != Z_STREAM_END);

    inflateEnd(&strm);
    return true;
}

HttpServer::HttpServer() : queue_producer_(nullptr) {}

HttpServer::HttpServer(std::shared_ptr<QueueProducer> queue_producer)
    : queue_producer_(queue_producer) {}

static inline std::string to_lower_trimmed(const std::string &s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    // trim spaces
    size_t start = out.find_first_not_of(' ');
    size_t end = out.find_last_not_of(' ');
    if (start == std::string::npos) return std::string();
    return out.substr(start, end - start + 1);
}

void HttpServer::setupRoutes(crow::SimpleApp& app) {
    auto queue_producer = queue_producer_;  // Capture for lambda

    // Health check endpoint for Kubernetes liveness probe
    CROW_ROUTE(app, "/health")
        ([](){
            return crow::response(200, "OK");
        });

    // Readiness check endpoint for Kubernetes readiness probe
    CROW_ROUTE(app, "/ready")
        ([queue_producer](){
            // Check if queue producer is initialized and ready
            if (queue_producer && !queue_producer->isReady()) {
                return crow::response(503, "Queue producer not ready");
            }
            return crow::response(200, "OK");
        });

    CROW_ROUTE(app, "/v1/logs")
        .methods("POST"_method)
        ([queue_producer](const crow::request& req){
            std::string content_type = req.get_header_value("Content-Type");
            // strip parameters like charset
            auto semipos = content_type.find(';');
            if (semipos != std::string::npos) content_type = content_type.substr(0, semipos);
            content_type = to_lower_trimmed(content_type);

            // Validate content type (but don't parse - defer to consumer)
            if (content_type != "application/x-protobuf" &&
                content_type != "application/protobuf" &&
                content_type != "application/json" &&
                content_type != "text/json") {
                return crow::response(415, "Unsupported Media Type");
            }

            // Decompress gzip if needed
            std::string body = req.body;
            std::string content_encoding = to_lower_trimmed(req.get_header_value("Content-Encoding"));
            if (content_encoding == "gzip") {
                std::string decompressed;
                if (!decompressGzip(req.body, decompressed)) {
                    return crow::response(400, "Failed to decompress gzip payload");
                }
                body.swap(decompressed);
            }

            // Create wrapper message with raw (decompressed) payload
            telemetry::v1::RawTelemetryMessage wrapper;
            wrapper.set_content_type(content_type);
            wrapper.set_telemetry_type(telemetry::v1::OTEL_LOGS);
            wrapper.set_payload(body);

            // Produce to queue if available
            if (queue_producer) {
                // Check backpressure before attempting to produce
                if (queue_producer->isAtCapacity()) {
                    return crow::response(429, "Too Many Requests: Queue is at capacity");
                }

                ProduceResult result = queue_producer->produce(wrapper);

                if (result == ProduceResult::QUEUE_FULL) {
                    return crow::response(503, "Service Unavailable: Queue is full");
                } else if (result == ProduceResult::PERSISTENT_ERROR) {
                    return crow::response(500, "Internal Server Error: Failed to queue message");
                } else if (result != ProduceResult::SUCCESS) {
                    return crow::response(503, "Service Unavailable: Queue error");
                }
            } else {
                // Fallback: just log (for testing without queue)
                std::cout << "Received RawTelemetryMessage with content_type="
                          << content_type << ", payload_size=" << body.size() << std::endl;
            }

            ExportLogsServiceResponse resp_msg;
            std::string resp_body;
            if (!resp_msg.SerializeToString(&resp_body)) {
                return crow::response(500, "Failed to serialize response");
            }
            crow::response res(200, resp_body);
            res.add_header("Content-Type", "application/x-protobuf");
            return res;
        });
}

void HttpServer::start(const std::string& host, int port) {
    crow::SimpleApp app;
    setupRoutes(app);

    std::cout << "OTel Log Receiver is running at http://" << host << ":" << port << std::endl;
    app.bindaddr(host).port(port).multithreaded().run();
}

