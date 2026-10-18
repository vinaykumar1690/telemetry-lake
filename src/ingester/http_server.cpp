#include "http_server.hpp"
#include "queue_producer.hpp"
#include "crow.h"
#include <algorithm>
#include <array>
#include <expected>
#include <format>
#include <iostream>
#include <limits>
#include <span>
#include <string_view>
#include <utility>
#include <zlib.h>
#include <google/protobuf/util/json_util.h>
#include "google/rpc/status.pb.h"
#include "telemetry_wrapper.pb.h"

namespace {

// Wire format of the request; OTLP/HTTP requires the response to use the same one
enum class Wire { Protobuf, Json };

struct MediaType {
    std::string_view name;  // canonical lowercase form, echoed in the response
    Wire wire;
};

constexpr std::array kSupportedMediaTypes{
    MediaType{"application/x-protobuf", Wire::Protobuf},
    MediaType{"application/protobuf", Wire::Protobuf},
    MediaType{"application/json", Wire::Json},
    MediaType{"text/json", Wire::Json},
};

// Used for errors raised before the request's media type is known
constexpr MediaType kDefaultMediaType = kSupportedMediaTypes[0];

constexpr std::array<std::string_view, 2> kIdentityEncodings{"", "identity"};
constexpr std::array<std::string_view, 2> kGzipEncodings{"gzip", "x-gzip"};

// google.rpc.Code values used in error bodies
constexpr int kGrpcInvalidArgument = 3;
constexpr int kGrpcInternal = 13;
constexpr int kGrpcUnavailable = 14;

// Seconds a client should wait before retrying a 503
constexpr std::string_view kRetryAfterSeconds = "1";

constexpr bool isOws(char c) { return c == ' ' || c == '\t'; }

constexpr char asciiLower(char c) {
    return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
}

// Trim HTTP optional whitespace (SP / HTAB)
constexpr std::string_view trimOws(std::string_view s) {
    while (!s.empty() && isOws(s.front())) s.remove_prefix(1);
    while (!s.empty() && isOws(s.back())) s.remove_suffix(1);
    return s;
}

constexpr bool iequals(std::string_view a, std::string_view b) {
    return std::ranges::equal(a, b, {}, asciiLower, asciiLower);
}

// Media type of a Content-Type header value, without parameters such as charset
const MediaType* findMediaType(std::string_view content_type) {
    const std::string_view media = trimOws(content_type.substr(0, content_type.find(';')));
    auto it = std::ranges::find_if(kSupportedMediaTypes,
                                   [&](const MediaType& m) { return iequals(m.name, media); });
    return it == kSupportedMediaTypes.end() ? nullptr : &*it;
}

bool isOneOf(std::string_view value, std::span<const std::string_view> candidates) {
    return std::ranges::contains(candidates, true,
                                 [&](std::string_view c) { return iequals(c, value); });
}

enum class DecodeError { Corrupt, TooLarge };

// Inflate a gzip body (including concatenated members, RFC 1952 §2.2), refusing
// to produce more than max_out bytes.
std::expected<std::string, DecodeError> decompressGzip(std::string_view in, std::size_t max_out) {
    std::string out;
    if (in.empty()) return out;
    if (in.size() > std::numeric_limits<uInt>::max()) {
        return std::unexpected(DecodeError::TooLarge);
    }

    z_stream strm{};
    // 16 + MAX_WBITS: expect a gzip header and trailer
    if (inflateInit2(&strm, 16 + MAX_WBITS) != Z_OK) {
        return std::unexpected(DecodeError::Corrupt);
    }
    struct InflateGuard {
        z_stream* s;
        ~InflateGuard() { inflateEnd(s); }
    } guard{&strm};

    strm.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(in.data()));
    strm.avail_in = static_cast<uInt>(in.size());

    // Size hint: ISIZE trailer (uncompressed size mod 2^32 of the last member)
    std::size_t hint = in.size() * 4;
    if (in.size() >= 18) {
        const auto* t = reinterpret_cast<const unsigned char*>(in.data() + in.size() - 4);
        hint = std::max<std::size_t>(hint, t[0] | (t[1] << 8) | (t[2] << 16) |
                                               (static_cast<std::size_t>(t[3]) << 24));
    }

    // Allow one byte past the limit so "exactly max_out" is distinguishable from "more"
    const std::size_t hard_cap = max_out + 1;
    std::size_t target = std::min(hard_cap, std::max<std::size_t>(hint, 4096));
    bool done = false;
    bool corrupt = false;

    while (!done) {
        const std::size_t old_size = out.size();
        const std::size_t new_size = std::min({hard_cap,
                                               std::max(target, old_size * 2),
                                               old_size + std::numeric_limits<uInt>::max()});
        out.resize_and_overwrite(new_size, [&](char* buf, std::size_t n) {
            strm.next_out = reinterpret_cast<Bytef*>(buf + old_size);
            strm.avail_out = static_cast<uInt>(n - old_size);
            while (strm.avail_out > 0) {
                const int ret = inflate(&strm, Z_NO_FLUSH);
                if (ret == Z_STREAM_END) {
                    if (strm.avail_in == 0) {
                        done = true;
                        break;
                    }
                    // Another gzip member follows
                    if (inflateReset(&strm) != Z_OK) {
                        corrupt = true;
                        break;
                    }
                    continue;
                }
                // Z_BUF_ERROR with output space left means the input ended mid-stream
                if (ret != Z_OK) {
                    corrupt = true;
                    break;
                }
            }
            return n - strm.avail_out;
        });

        if (corrupt) return std::unexpected(DecodeError::Corrupt);
        if (out.size() > max_out) return std::unexpected(DecodeError::TooLarge);
    }
    return out;
}

crow::response makeResponse(int code, std::string_view content_type, std::string body) {
    crow::response res(code, std::move(body));
    res.set_header("Content-Type", std::string(content_type));
    return res;
}

// Successful export: an empty ExportLogsServiceResponse, which serializes to ""
// in protobuf and "{}" in JSON
crow::response okResponse(const MediaType& media) {
    return makeResponse(200, media.name, media.wire == Wire::Json ? "{}" : "");
}

// OTLP/HTTP error: google.rpc.Status encoded like the request
crow::response errorResponse(const MediaType& media, int http_code, int grpc_code,
                             std::string_view message) {
    google::rpc::Status status;
    status.set_code(grpc_code);
    status.set_message(std::string(message));

    std::string body;
    if (media.wire == Wire::Json) {
        if (!google::protobuf::util::MessageToJsonString(status, &body).ok()) body = "{}";
    } else {
        status.SerializeToString(&body);
    }

    crow::response res = makeResponse(http_code, media.name, std::move(body));
    if (http_code == 503) {
        res.set_header("Retry-After", std::string(kRetryAfterSeconds));
    }
    return res;
}

crow::response unavailableResponse(const MediaType& media) {
    return errorResponse(media, 503, kGrpcUnavailable,
                         "Service Unavailable: queue is at capacity, retry later");
}

}  // namespace

HttpServer::HttpServer() = default;

HttpServer::HttpServer(std::shared_ptr<QueueProducer> queue_producer, HttpServerOptions options)
    : queue_producer_(std::move(queue_producer)), options_(std::move(options)) {}

void HttpServer::setupRoutes(crow::SimpleApp& app) {
    auto queue_producer = queue_producer_;  // Capture for lambda
    const auto options = options_;

    // Health check endpoint for Kubernetes liveness probe
    CROW_ROUTE(app, "/health")
        ([](){
            return crow::response(200, "OK");
        });

    // Readiness check endpoint for Kubernetes readiness probe
    CROW_ROUTE(app, "/ready")
        ([queue_producer](){
            // Not ready without a working producer: accepted data would be dropped
            if (!queue_producer || !queue_producer->isReady()) {
                return crow::response(503, "Queue producer not ready");
            }
            return crow::response(200, "OK");
        });

    CROW_ROUTE(app, "/v1/logs")
        .methods("POST"_method)
        ([queue_producer, options](const crow::request& req){
            if (req.body.size() > options.max_body_bytes) {
                return errorResponse(kDefaultMediaType, 413, kGrpcInvalidArgument,
                                     "Payload Too Large");
            }

            // Validate content type (but don't parse - defer to consumer)
            const MediaType* media = findMediaType(req.get_header_value("Content-Type"));
            if (!media) {
                return errorResponse(kDefaultMediaType, 415, kGrpcInvalidArgument,
                                     "Unsupported Media Type");
            }

            const std::string_view encoding = trimOws(req.get_header_value("Content-Encoding"));
            const bool gzip = isOneOf(encoding, kGzipEncodings);
            if (!gzip && !isOneOf(encoding, kIdentityEncodings)) {
                return errorResponse(*media, 415, kGrpcInvalidArgument,
                                     "Unsupported Content-Encoding");
            }

            // Shed load before spending CPU on decompression
            if (queue_producer && queue_producer->isAtCapacity()) {
                return unavailableResponse(*media);
            }

            // Create wrapper message with raw (decompressed) payload
            telemetry::v1::RawTelemetryMessage wrapper;
            wrapper.set_content_type(std::string(media->name));
            wrapper.set_telemetry_type(telemetry::v1::OTEL_LOGS);
            if (gzip) {
                auto decompressed = decompressGzip(req.body, options.max_decompressed_bytes);
                if (!decompressed) {
                    switch (decompressed.error()) {
                        case DecodeError::TooLarge:
                            return errorResponse(*media, 413, kGrpcInvalidArgument,
                                                 "Payload Too Large after decompression");
                        case DecodeError::Corrupt:
                            return errorResponse(*media, 400, kGrpcInvalidArgument,
                                                 "Failed to decompress gzip payload");
                    }
                    std::unreachable();
                }
                wrapper.set_payload(std::move(*decompressed));
            } else {
                wrapper.set_payload(req.body);
            }

            // Without a producer (tests only; main() exits instead), accept and discard
            if (!queue_producer) {
                return okResponse(*media);
            }

            switch (queue_producer->produce(wrapper)) {
                case ProduceResult::SUCCESS:
                    return okResponse(*media);
                case ProduceResult::QUEUE_FULL:
                    return unavailableResponse(*media);
                case ProduceResult::RETRYABLE_ERROR:
                    return errorResponse(*media, 503, kGrpcUnavailable,
                                         "Service Unavailable: queue error, retry later");
                case ProduceResult::PAYLOAD_TOO_LARGE:
                    return errorResponse(*media, 413, kGrpcInvalidArgument,
                                         "Payload Too Large for queue");
                case ProduceResult::PERSISTENT_ERROR:
                    return errorResponse(*media, 500, kGrpcInternal,
                                         "Internal Server Error: failed to queue message");
            }
            std::unreachable();
        });
}

void HttpServer::start() {
    crow::SimpleApp app;
    app.loglevel(options_.log_level);
    setupRoutes(app);

    app.bindaddr(options_.host)
        .port(options_.port)
        .server_name(options_.server_name);
    if (options_.threads > 0) {
        app.concurrency(options_.threads);
    } else {
        app.multithreaded();
    }

    std::cout << std::format("OTel Log Receiver is running at http://{}:{}\n",
                             options_.host, options_.port);
    app.run();
}
