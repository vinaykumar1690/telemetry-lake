#include <gtest/gtest.h>
#include "http_server.hpp"
#include "crow.h"
#include <zlib.h>
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
    EXPECT_EQ(res.get_header_value("Content-Type"), "application/x-protobuf");
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
    EXPECT_EQ(res.get_header_value("Content-Type"), "application/x-protobuf");
    
    // Verify response is valid protobuf
    ExportLogsServiceResponse response;
    EXPECT_TRUE(response.ParseFromString(res.body));
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
    EXPECT_EQ(res.get_header_value("Content-Type"), "application/x-protobuf");
    
    // Verify response is valid protobuf
    ExportLogsServiceResponse response;
    EXPECT_TRUE(response.ParseFromString(res.body));
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
    EXPECT_EQ(res.get_header_value("Content-Type"), "application/x-protobuf");
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

// Test invalid protobuf payload
TEST_F(HttpServerTest, RejectsInvalidProtobufPayload) {
    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = "invalid protobuf data";
    req.add_header("Content-Type", "application/x-protobuf");

    crow::response res;
    app.handle_full(req, res);

    EXPECT_EQ(res.code, 400);
    EXPECT_EQ(res.body, "Invalid Protobuf payload");
}

// Test invalid JSON payload
TEST_F(HttpServerTest, RejectsInvalidJsonPayload) {
    crow::request req;
    req.url = "/v1/logs";
    req.method = "POST"_method;
    req.body = "{ invalid json }";
    req.add_header("Content-Type", "application/json");

    crow::response res;
    app.handle_full(req, res);

    EXPECT_EQ(res.code, 400);
    EXPECT_NE(res.body.find("Invalid JSON payload"), std::string::npos);
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
    EXPECT_EQ(res.body, "Failed to decompress gzip payload");
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

