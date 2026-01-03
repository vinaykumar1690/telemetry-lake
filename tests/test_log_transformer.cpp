#include <gtest/gtest.h>
#include "../src/appender/log_transformer.hpp"
#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"
#include "opentelemetry/proto/logs/v1/logs.pb.h"
#include "opentelemetry/proto/resource/v1/resource.pb.h"
#include "opentelemetry/proto/common/v1/common.pb.h"

TEST(LogTransformerTest, BasicTransformation) {
    opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest request;
    
    auto* resource_logs = request.add_resource_logs();
    auto* resource = resource_logs->mutable_resource();
    
    // Add service.name attribute
    auto* attr = resource->add_attributes();
    attr->set_key("service.name");
    attr->mutable_value()->set_string_value("test-service");
    
    auto* scope_logs = resource_logs->add_scope_logs();
    auto* log_record = scope_logs->add_log_records();
    
    log_record->set_time_unix_nano(1672531200000000000ULL); // 2023-01-01 00:00:00 UTC
    log_record->set_severity_text("INFO");
    log_record->mutable_body()->set_string_value("Test log message");
    
    auto transformed = LogTransformer::transform(request);
    
    ASSERT_EQ(transformed.size(), 1);
    
    EXPECT_EQ(transformed[0].severity, "INFO");
    EXPECT_EQ(transformed[0].body, "Test log message");
    EXPECT_EQ(transformed[0].service_name, "test-service");
}

TEST(LogTransformerTest, WellKnownAttributes) {
    opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest request;
    
    auto* resource_logs = request.add_resource_logs();
    auto* resource = resource_logs->mutable_resource();
    
    // Add well-known attributes
    auto* attr1 = resource->add_attributes();
    attr1->set_key("service.name");
    attr1->mutable_value()->set_string_value("my-service");
    
    auto* attr2 = resource->add_attributes();
    attr2->set_key("deployment.environment");
    attr2->mutable_value()->set_string_value("production");
    
    auto* attr3 = resource->add_attributes();
    attr3->set_key("host.name");
    attr3->mutable_value()->set_string_value("host-123");
    
    // Add other attribute (should go to map)
    auto* attr4 = resource->add_attributes();
    attr4->set_key("custom.attr");
    attr4->mutable_value()->set_string_value("custom-value");
    
    auto* scope_logs = resource_logs->add_scope_logs();
    auto* log_record = scope_logs->add_log_records();
    log_record->set_time_unix_nano(1672531200000000000ULL);
    
    auto transformed = LogTransformer::transform(request);
    
    ASSERT_EQ(transformed.size(), 1);
    
    EXPECT_EQ(transformed[0].service_name, "my-service");
    EXPECT_EQ(transformed[0].deployment_environment, "production");
    EXPECT_EQ(transformed[0].host_name, "host-123");
    EXPECT_EQ(transformed[0].attributes["custom.attr"], "custom-value");
}

TEST(LogTransformerTest, TraceIdSpanId) {
    opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest request;
    
    auto* resource_logs = request.add_resource_logs();
    auto* scope_logs = resource_logs->add_scope_logs();
    auto* log_record = scope_logs->add_log_records();
    
    log_record->set_time_unix_nano(1672531200000000000ULL);
    log_record->set_trace_id("\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10");
    log_record->set_span_id("\x01\x02\x03\x04\x05\x06\x07\x08");
    
    auto transformed = LogTransformer::transform(request);
    
    ASSERT_EQ(transformed.size(), 1);
    EXPECT_EQ(transformed[0].trace_id, "0102030405060708090a0b0c0d0e0f10");
    EXPECT_EQ(transformed[0].span_id, "0102030405060708");
}

TEST(LogTransformerTest, SeverityFromNumber) {
    opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest request;
    
    auto* resource_logs = request.add_resource_logs();
    auto* scope_logs = resource_logs->add_scope_logs();
    auto* log_record = scope_logs->add_log_records();
    
    log_record->set_time_unix_nano(1672531200000000000ULL);
    log_record->set_severity_number(opentelemetry::proto::logs::v1::SEVERITY_NUMBER_ERROR);
    // No severity_text set
    
    auto transformed = LogTransformer::transform(request);
    
    ASSERT_EQ(transformed.size(), 1);
    EXPECT_EQ(transformed[0].severity, "ERROR");
}

