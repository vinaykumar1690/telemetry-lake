#include <gtest/gtest.h>
#include "ingester/http_server.hpp"
#include "ingester/ingester_metrics.hpp"
#include "crow.h"
#include <zlib.h>
#include <memory>
#include <google/protobuf/util/json_util.h>
#include "google/rpc/status.pb.h"
#include "telemetry_wrapper.pb.h"
#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"

using opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
using opentelemetry::proto::collector::logs::v1::ExportLogsServiceResponse;

// Helper function to compress data using gzip
static std::string compressGzip(const std::string &in) {
    if (in.empty()) return std::string();
    
    z_stream strm{};
    // 16 + MAX_WBITS to enable gzip encoding
    if (deflateInit2(&strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 16 + MAX_WBITS, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
        return std::string();
    }

    strm.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(in.data()));
    strm.avail_in = static_cast<uInt>(in.size());

    char buf[4096];
    std::string out;
    int ret;
    
    do {
        strm.next_out = reinterpret_cast<Bytef*>(buf);
        strm.avail_out = sizeof(buf);
        ret = deflate(&strm, Z_FINISH);
        if (ret != Z_OK && ret != Z_STREAM_END) {
            deflateEnd(&strm);
            return std::string();
        }
        size_t have = sizeof(buf) - strm.avail_out;
        out.append(buf, have);
    } while (ret != Z_STREAM_END);

    deflateEnd(&strm);
    return out;
}


// Test fixture for HTTP server tests
class HttpServerTest : public ::testing::Test {
protected:
    void SetUp() override {
        server.setupRoutes(app);
        app.validate();  // Required to make sure all route handlers are in order
    }

    HttpServer server;
    crow::SimpleApp app;
};

// Test protobuf content type without gzip
TEST_F(HttpServerTest, HandlesProtobufContentType) {
    ExportLogsServiceRequest request;
    auto* resource_logs = request.add_resource_logs();
    resource_logs->mutable_resource()->add_attributes()->set_key("service.name");
    
    std::string serialized;
    ASSERT_TRUE(request.SerializeToString(&serialized));

    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = serialized;
    req.add_header("Content-Type", "application/x-protobuf");

    crow::response res;
    app.handle_full(req, res);

    EXPECT_EQ(res.code, 200);
    EXPECT_EQ(res.get_header_value("Content-Type"), "application/x-protobuf");
    
    // Verify response is valid protobuf
    ExportLogsServiceResponse response;
    EXPECT_TRUE(response.ParseFromString(res.body));
}

// Test protobuf content type with gzip
TEST_F(HttpServerTest, HandlesProtobufContentTypeWithGzip) {
    ExportLogsServiceRequest request;
    auto* resource_logs = request.add_resource_logs();
    resource_logs->mutable_resource()->add_attributes()->set_key("service.name");
    
    std::string serialized;
    ASSERT_TRUE(request.SerializeToString(&serialized));
    
    std::string compressed = compressGzip(serialized);
    ASSERT_FALSE(compressed.empty());

    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = compressed;
    req.add_header("Content-Type", "application/x-protobuf");
    req.add_header("Content-Encoding", "gzip");

    crow::response res;
    app.handle_full(req, res);

    EXPECT_EQ(res.code, 200);
    EXPECT_EQ(res.get_header_value("Content-Type"), "application/x-protobuf");
    
    // Verify response is valid protobuf
    ExportLogsServiceResponse response;
    EXPECT_TRUE(response.ParseFromString(res.body));
}

// Test application/protobuf content type (alternative)
TEST_F(HttpServerTest, HandlesApplicationProtobufContentType) {
    ExportLogsServiceRequest request;
    auto* resource_logs = request.add_resource_logs();
    resource_logs->mutable_resource()->add_attributes()->set_key("service.name");
    
    std::string serialized;
    ASSERT_TRUE(request.SerializeToString(&serialized));

    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = serialized;
    req.add_header("Content-Type", "application/protobuf");

    crow::response res;
    app.handle_full(req, res);

    EXPECT_EQ(res.code, 200);
    EXPECT_EQ(res.get_header_value("Content-Type"), "application/protobuf");
}

// Test JSON content type without gzip
TEST_F(HttpServerTest, HandlesJsonContentType) {
    std::string json_data = R"({
        "resourceLogs": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": {
                        "stringValue": "test-service"
                    }
                }]
            },
            "scopeLogs": [{
                "scope": {},
                "logRecords": [{
                    "timeUnixNano": "1672531200000000000",
                    "severityText": "INFO",
                    "body": {
                        "stringValue": "Test log message"
                    }
                }]
            }]
        }]
    })";

    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = json_data;
    req.add_header("Content-Type", "application/json");

    crow::response res;
    app.handle_full(req, res);

    EXPECT_EQ(res.code, 200);
    EXPECT_EQ(res.get_header_value("Content-Type"), "application/json");
    EXPECT_EQ(res.body, "{}");
}

// Test JSON content type with gzip
TEST_F(HttpServerTest, HandlesJsonContentTypeWithGzip) {
    std::string json_data = R"({
        "resourceLogs": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": {
                        "stringValue": "test-service"
                    }
                }]
            },
            "scopeLogs": [{
                "scope": {},
                "logRecords": [{
                    "timeUnixNano": "1672531200000000000",
                    "severityText": "INFO",
                    "body": {
                        "stringValue": "Test log message"
                    }
                }]
            }]
        }]
    })";

    std::string compressed = compressGzip(json_data);
    ASSERT_FALSE(compressed.empty());

    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = compressed;
    req.add_header("Content-Type", "application/json");
    req.add_header("Content-Encoding", "gzip");

    crow::response res;
    app.handle_full(req, res);

    EXPECT_EQ(res.code, 200);
    EXPECT_EQ(res.get_header_value("Content-Type"), "application/json");
    EXPECT_EQ(res.body, "{}");
}

// Test text/json content type (alternative)
TEST_F(HttpServerTest, HandlesTextJsonContentType) {
    std::string json_data = R"({
        "resourceLogs": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": {
                        "stringValue": "test-service"
                    }
                }]
            }
        }]
    })";

    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = json_data;
    req.add_header("Content-Type", "text/json");

    crow::response res;
    app.handle_full(req, res);

    EXPECT_EQ(res.code, 200);
    EXPECT_EQ(res.get_header_value("Content-Type"), "text/json");
    EXPECT_EQ(res.body, "{}");
}

// Test Content-Type with charset parameter
TEST_F(HttpServerTest, HandlesContentTypeWithCharset) {
    ExportLogsServiceRequest request;
    auto* resource_logs = request.add_resource_logs();
    resource_logs->mutable_resource()->add_attributes()->set_key("service.name");
    
    std::string serialized;
    ASSERT_TRUE(request.SerializeToString(&serialized));

    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = serialized;
    req.add_header("Content-Type", "application/x-protobuf; charset=utf-8");

    crow::response res;
    app.handle_full(req, res);

    EXPECT_EQ(res.code, 200);
}

// Test unsupported media type
TEST_F(HttpServerTest, RejectsUnsupportedMediaType) {
    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = "some data";
    req.add_header("Content-Type", "text/plain");

    crow::response res;
    app.handle_full(req, res);

    EXPECT_EQ(res.code, 415);
}

// Test invalid protobuf payload - now accepted (validation deferred to consumer)
TEST_F(HttpServerTest, AcceptsInvalidProtobufPayload) {
    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = "invalid protobuf data";
    req.add_header("Content-Type", "application/x-protobuf");

    crow::response res;
    app.handle_full(req, res);

    // HTTP server no longer validates payload - deferred to consumer
    EXPECT_EQ(res.code, 200);
}

// Test invalid JSON payload - now accepted (validation deferred to consumer)
TEST_F(HttpServerTest, AcceptsInvalidJsonPayload) {
    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = "{ invalid json }";
    req.add_header("Content-Type", "application/json");

    crow::response res;
    app.handle_full(req, res);

    // HTTP server no longer validates payload - deferred to consumer
    EXPECT_EQ(res.code, 200);
}

// Test invalid gzip payload
TEST_F(HttpServerTest, RejectsInvalidGzipPayload) {
    ExportLogsServiceRequest request;
    auto* resource_logs = request.add_resource_logs();
    resource_logs->mutable_resource()->add_attributes()->set_key("service.name");
    
    std::string serialized;
    ASSERT_TRUE(request.SerializeToString(&serialized));

    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = "invalid gzip data";
    req.add_header("Content-Type", "application/x-protobuf");
    req.add_header("Content-Encoding", "gzip");

    crow::response res;
    app.handle_full(req, res);

    EXPECT_EQ(res.code, 400);
    EXPECT_EQ(res.get_header_value("Content-Type"), "application/x-protobuf");
    google::rpc::Status status;
    ASSERT_TRUE(status.ParseFromString(res.body));
    EXPECT_EQ(status.code(), 3);  // INVALID_ARGUMENT
    EXPECT_EQ(status.message(), "Failed to decompress gzip payload");
}

// Test empty gzip payload
TEST_F(HttpServerTest, HandlesEmptyGzipPayload) {
    std::string empty_compressed = compressGzip("");
    
    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = empty_compressed;
    req.add_header("Content-Type", "application/x-protobuf");
    req.add_header("Content-Encoding", "gzip");

    crow::response res;
    app.handle_full(req, res);

    // Empty protobuf should still parse (but may be invalid)
    // The server should handle it gracefully
    EXPECT_EQ(res.code, 200);
}

// Test Content-Encoding case insensitivity
TEST_F(HttpServerTest, HandlesCaseInsensitiveContentEncoding) {
    ExportLogsServiceRequest request;
    auto* resource_logs = request.add_resource_logs();
    resource_logs->mutable_resource()->add_attributes()->set_key("service.name");
    
    std::string serialized;
    ASSERT_TRUE(request.SerializeToString(&serialized));
    
    std::string compressed = compressGzip(serialized);
    ASSERT_FALSE(compressed.empty());

    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = compressed;
    req.add_header("Content-Type", "application/x-protobuf");
    req.add_header("Content-Encoding", "GZIP");  // Uppercase

    crow::response res;
    app.handle_full(req, res);

    EXPECT_EQ(res.code, 200);
}

// Test Content-Type case insensitivity
TEST_F(HttpServerTest, HandlesCaseInsensitiveContentType) {
    ExportLogsServiceRequest request;
    auto* resource_logs = request.add_resource_logs();
    resource_logs->mutable_resource()->add_attributes()->set_key("service.name");
    
    std::string serialized;
    ASSERT_TRUE(request.SerializeToString(&serialized));

    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = serialized;
    req.add_header("Content-Type", "APPLICATION/X-PROTOBUF");  // Uppercase

    crow::response res;
    app.handle_full(req, res);

    EXPECT_EQ(res.code, 200);
}

// Fixture for tests that need non-default server limits
class HttpServerLimitsTest : public ::testing::Test {
protected:
    void configure(HttpServerOptions options) {
        server = std::make_unique<HttpServer>(nullptr, std::move(options));
        server->setupRoutes(app);
        app.validate();
    }

    crow::response post(const std::string& body, const std::string& content_type,
                        const std::string& content_encoding = "") {
        crow::request req;
        req.url = "/v1/logs";
        req.method = "POST"_method;
        req.body = body;
        req.add_header("Content-Type", content_type);
        if (!content_encoding.empty()) req.add_header("Content-Encoding", content_encoding);
        crow::response res;
        app.handle_full(req, res);
        return res;
    }

    std::unique_ptr<HttpServer> server;
    crow::SimpleApp app;
};

static google::rpc::Status parseStatus(const crow::response& res) {
    google::rpc::Status status;
    EXPECT_TRUE(status.ParseFromString(res.body));
    return status;
}

// Raw body larger than max_body_bytes is rejected before any processing
TEST_F(HttpServerLimitsTest, RejectsBodyOverLimit) {
    HttpServerOptions options;
    options.max_body_bytes = 16;
    configure(options);

    EXPECT_EQ(post(std::string(16, 'x'), "application/x-protobuf").code, 200);

    crow::response res = post(std::string(17, 'x'), "application/x-protobuf");
    EXPECT_EQ(res.code, 413);
    EXPECT_EQ(parseStatus(res).message(), "Payload Too Large");
}

// A small gzip body that inflates past max_decompressed_bytes is rejected
TEST_F(HttpServerLimitsTest, RejectsGzipBomb) {
    HttpServerOptions options;
    options.max_decompressed_bytes = 64 * 1024;
    configure(options);

    std::string bomb = compressGzip(std::string(8 * 1024 * 1024, '\0'));
    ASSERT_LT(bomb.size(), 64u * 1024);

    crow::response res = post(bomb, "application/x-protobuf", "gzip");
    EXPECT_EQ(res.code, 413);
    EXPECT_EQ(parseStatus(res).message(), "Payload Too Large after decompression");
}

// Decompressed size exactly at the limit is accepted, one byte over is not
TEST_F(HttpServerLimitsTest, DecompressedLimitIsInclusive) {
    HttpServerOptions options;
    options.max_decompressed_bytes = 100000;
    configure(options);

    EXPECT_EQ(post(compressGzip(std::string(100000, 'a')), "application/x-protobuf", "gzip").code, 200);
    EXPECT_EQ(post(compressGzip(std::string(100001, 'a')), "application/x-protobuf", "gzip").code, 413);
}

// Large payload exercises buffer growth across several inflate rounds
TEST_F(HttpServerLimitsTest, HandlesLargeGzipPayload) {
    configure({});

    std::string data;
    data.reserve(4 * 1024 * 1024);
    uint32_t x = 12345;
    while (data.size() < 4 * 1024 * 1024) {
        x = x * 1103515245 + 12345;
        data.push_back(static_cast<char>('a' + (x >> 16) % 26));
    }
    EXPECT_EQ(post(compressGzip(data), "application/x-protobuf", "gzip").code, 200);
}

// Concatenated gzip members (RFC 1952) are all inflated, not just the first
TEST_F(HttpServerLimitsTest, HandlesMultiMemberGzip) {
    HttpServerOptions options;
    options.max_decompressed_bytes = 11;  // "hello " + "world"
    configure(options);

    std::string two_members = compressGzip("hello ") + compressGzip("world");
    EXPECT_EQ(post(two_members, "application/x-protobuf", "gzip").code, 200);

    // A third member pushes the total past the limit, proving later members are read
    std::string three_members = two_members + compressGzip("!");
    EXPECT_EQ(post(three_members, "application/x-protobuf", "gzip").code, 413);
}

TEST_F(HttpServerLimitsTest, RejectsTruncatedGzip) {
    configure({});
    std::string gz = compressGzip(std::string(1000, 'a'));
    gz.resize(gz.size() - 5);

    crow::response res = post(gz, "application/x-protobuf", "gzip");
    EXPECT_EQ(res.code, 400);
    EXPECT_EQ(parseStatus(res).message(), "Failed to decompress gzip payload");
}

TEST_F(HttpServerLimitsTest, RejectsTrailingGarbageAfterGzip) {
    configure({});
    EXPECT_EQ(post(compressGzip("payload") + "garbage", "application/x-protobuf", "gzip").code, 400);
}

// Content codings other than gzip/identity must not be forwarded still encoded
TEST_F(HttpServerLimitsTest, RejectsUnsupportedContentEncoding) {
    configure({});
    for (const char* encoding : {"br", "deflate", "zstd", "gzip, br"}) {
        crow::response res = post("payload", "application/x-protobuf", encoding);
        EXPECT_EQ(res.code, 415) << encoding;
        EXPECT_EQ(parseStatus(res).message(), "Unsupported Content-Encoding") << encoding;
    }
}

TEST_F(HttpServerLimitsTest, AcceptsIdentityAndXGzipEncodings) {
    configure({});
    EXPECT_EQ(post("payload", "application/x-protobuf", "identity").code, 200);
    EXPECT_EQ(post(compressGzip("payload"), "application/x-protobuf", "x-gzip").code, 200);
}

// Error bodies use the request's encoding: JSON request -> JSON google.rpc.Status
TEST_F(HttpServerLimitsTest, JsonErrorBodyIsJsonStatus) {
    configure({});
    crow::response res = post("not gzip", "application/json", "gzip");
    EXPECT_EQ(res.code, 400);
    EXPECT_EQ(res.get_header_value("Content-Type"), "application/json");

    google::rpc::Status status;
    ASSERT_TRUE(google::protobuf::util::JsonStringToMessage(res.body, &status).ok()) << res.body;
    EXPECT_EQ(status.code(), 3);
    EXPECT_EQ(status.message(), "Failed to decompress gzip payload");
}

// Unknown media type: error falls back to protobuf-encoded Status
TEST_F(HttpServerLimitsTest, UnsupportedMediaTypeReturnsProtobufStatus) {
    configure({});
    crow::response res = post("payload", "text/plain");
    EXPECT_EQ(res.code, 415);
    EXPECT_EQ(res.get_header_value("Content-Type"), "application/x-protobuf");
    EXPECT_EQ(parseStatus(res).message(), "Unsupported Media Type");
}

// HTTP optional whitespace includes horizontal tabs
TEST_F(HttpServerLimitsTest, HandlesTabsInContentType) {
    configure({});
    for (const char* content_type : {"application/json;\tcharset=utf-8",
                                     "\tapplication/json\t; charset=utf-8",
                                     " Application/JSON "}) {
        crow::response res = post("{}", content_type);
        EXPECT_EQ(res.code, 200) << content_type;
        EXPECT_EQ(res.get_header_value("Content-Type"), "application/json") << content_type;
    }
}

TEST_F(HttpServerLimitsTest, RejectsNonPostMethod) {
    configure({});
    crow::request req;
    req.url = "/v1/logs";
    req.method = "GET"_method;
    crow::response res;
    app.handle_full(req, res);
    EXPECT_EQ(res.code, 405);
}

TEST_F(HttpServerTest, HealthIsOkWithoutProducer) {
    crow::request req;
    req.url = "/health";
    crow::response res;
    app.handle_full(req, res);
    EXPECT_EQ(res.code, 200);
}

// A pod without a working producer would drop data, so it must not report ready
TEST_F(HttpServerTest, ReadyReturns503WithoutProducer) {
    crow::request req;
    req.url = "/ready";
    crow::response res;
    app.handle_full(req, res);
    EXPECT_EQ(res.code, 503);
}

// Metrics: every response path is counted, rejects carry their reason, and
// bytes are counted as received and after decompression
TEST_F(HttpServerLimitsTest, CountsRequestsBytesAndRejects) {
    HttpServerOptions options;
    options.max_body_bytes = 64;
    configure(options);
    auto& m = server->metrics();

    const std::string payload(40, 'x');
    const std::string gzipped = compressGzip(payload);
    EXPECT_EQ(post(payload, "application/x-protobuf").code, 200);
    EXPECT_EQ(post(gzipped, "application/json", "gzip").code, 200);
    EXPECT_EQ(post("{}", "text/plain").code, 415);
    EXPECT_EQ(post("{}", "application/json", "br").code, 415);
    EXPECT_EQ(post(std::string(65, 'x'), "application/json").code, 413);
    EXPECT_EQ(post("not gzip", "application/json", "gzip").code, 400);

    auto requests = [&](const char* code) {
        return m.requests.Add({{"route", "/v1/logs"}, {"code", code}}).Value();
    };
    EXPECT_EQ(requests("200"), 2);
    EXPECT_EQ(requests("415"), 2);
    EXPECT_EQ(requests("413"), 1);
    EXPECT_EQ(requests("400"), 1);

    auto rejected = [&](const char* reason) { return m.rejected.Add({{"reason", reason}}).Value(); };
    EXPECT_EQ(rejected("unsupported_media_type"), 1);
    EXPECT_EQ(rejected("unsupported_encoding"), 1);
    EXPECT_EQ(rejected("too_large"), 1);
    EXPECT_EQ(rejected("bad_gzip"), 1);

    EXPECT_EQ(m.received_identity_bytes.Value(), 40);
    EXPECT_EQ(m.received_gzip_bytes.Value(), gzipped.size() + std::string("not gzip").size());
    EXPECT_EQ(m.payload_protobuf_bytes.Value(), 40);
    EXPECT_EQ(m.payload_json_bytes.Value(), 40);
    EXPECT_EQ(m.logs_in_flight.Value(), 0);

    const auto duration = m.logs_duration.Collect().histogram;
    EXPECT_EQ(duration.sample_count, 6u);
    EXPECT_EQ(m.payload_size.Collect().histogram.sample_count, 2u);
}

TEST_F(HttpServerTest, CountsProbes) {
    for (const char* url : {"/health", "/ready"}) {
        crow::request req;
        req.url = url;
        crow::response res;
        app.handle_full(req, res);
    }
    EXPECT_EQ(server.metrics().requests.Add({{"route", "/health"}, {"code", "200"}}).Value(), 1);
    EXPECT_EQ(server.metrics().requests.Add({{"route", "/ready"}, {"code", "503"}}).Value(), 1);
}
