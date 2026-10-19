#include "appender/log_transformer.hpp"
#include "appender/otlp_decoder.hpp"
#include "telemetry_wrapper.pb.h"

#include <gtest/gtest.h>

#include <limits>
#include <string>

using namespace appender;
using opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
using opentelemetry::proto::common::v1::AnyValue;
using opentelemetry::proto::logs::v1::LogRecord;

namespace {

void addAttr(google::protobuf::RepeatedPtrField<opentelemetry::proto::common::v1::KeyValue>* attrs,
             const std::string& key, const std::string& value) {
    auto* kv = attrs->Add();
    kv->set_key(key);
    kv->mutable_value()->set_string_value(value);
}

ExportLogsServiceRequest sampleRequest() {
    ExportLogsServiceRequest request;
    auto* resource_logs = request.add_resource_logs();
    auto* resource_attrs = resource_logs->mutable_resource()->mutable_attributes();
    addAttr(resource_attrs, "service.name", "checkout");
    addAttr(resource_attrs, "service.namespace", "shop");
    addAttr(resource_attrs, "service.instance.id", "pod-1");
    addAttr(resource_attrs, "deployment.environment.name", "prod");
    addAttr(resource_attrs, "host.name", "node-7");
    addAttr(resource_attrs, "cloud.region", "eu-west-1");

    auto* scope_logs = resource_logs->add_scope_logs();
    scope_logs->mutable_scope()->set_name("io.example.logger");
    scope_logs->mutable_scope()->set_version("1.2.3");

    auto* record = scope_logs->add_log_records();
    record->set_time_unix_nano(1'700'000'000'123'456'789ULL);
    record->set_observed_time_unix_nano(1'700'000'001'000'000'000ULL);
    record->set_severity_number(opentelemetry::proto::logs::v1::SEVERITY_NUMBER_WARN2);
    record->mutable_body()->set_string_value(R"(path C:\tmp it's "quoted")");
    record->set_trace_id(std::string("\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10", 16));
    record->set_span_id(std::string("\xaa\xbb\xcc\xdd\xee\xff\x00\x11", 8));
    record->set_flags(1);
    record->set_event_name("checkout.completed");
    record->set_dropped_attributes_count(2);
    addAttr(record->mutable_attributes(), "user.id", "42");
    return request;
}

std::string wrap(const std::string& content_type, const std::string& payload) {
    telemetry::v1::RawTelemetryMessage wrapper;
    wrapper.set_content_type(content_type);
    wrapper.set_telemetry_type(telemetry::v1::OTEL_LOGS);
    wrapper.set_payload(payload);
    return wrapper.SerializeAsString();
}

}  // namespace

TEST(LogTransformerTest, FlattensAllFields) {
    std::vector<LogRow> rows;
    transformLogs(sampleRequest(), {.partition = 3, .offset = 99}, rows);
    ASSERT_EQ(rows.size(), 1u);
    const LogRow& row = rows[0];

    EXPECT_EQ(row.kafka_partition, 3);
    EXPECT_EQ(row.kafka_offset, 99);
    EXPECT_EQ(row.time_unix_nano, 1'700'000'000'123'456'789ULL);
    EXPECT_EQ(row.timestamp_us, 1'700'000'000'123'456LL);
    EXPECT_EQ(row.severity_number, 14);
    EXPECT_EQ(row.severity_text, "WARN");
    EXPECT_EQ(row.body, R"(path C:\tmp it's "quoted")");
    EXPECT_EQ(row.trace_id, "0102030405060708090a0b0c0d0e0f10");
    EXPECT_EQ(row.span_id, "aabbccddeeff0011");
    EXPECT_EQ(row.flags, 1u);
    EXPECT_EQ(row.event_name, "checkout.completed");
    EXPECT_EQ(row.dropped_attributes_count, 2u);

    ASSERT_TRUE(row.resource);
    EXPECT_EQ(row.resource->service_name, "checkout");
    EXPECT_EQ(row.resource->service_namespace, "shop");
    EXPECT_EQ(row.resource->service_instance_id, "pod-1");
    EXPECT_EQ(row.resource->deployment_environment, "prod");
    EXPECT_EQ(row.resource->host_name, "node-7");
    EXPECT_EQ(row.resource->attributes.size(), 6u);  // promoted keys stay in the map

    ASSERT_TRUE(row.scope);
    EXPECT_EQ(row.scope->name, "io.example.logger");
    EXPECT_EQ(row.scope->version, "1.2.3");

    ASSERT_EQ(row.attributes.size(), 1u);
    EXPECT_EQ(row.attributes[0], (std::pair<std::string, std::string>{"user.id", "42"}));
}

TEST(LogTransformerTest, SharesResourceAndScopeAcrossRecords) {
    auto request = sampleRequest();
    auto* scope_logs = request.mutable_resource_logs(0)->mutable_scope_logs(0);
    *scope_logs->add_log_records() = scope_logs->log_records(0);
    std::vector<LogRow> rows;
    transformLogs(request, {}, rows);
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0].resource.get(), rows[1].resource.get());
    EXPECT_EQ(rows[0].scope.get(), rows[1].scope.get());
}

TEST(LogTransformerTest, NumbersRecordsWithinTheMessage) {
    auto request = sampleRequest();
    auto* resource_logs = request.add_resource_logs();
    *resource_logs = request.resource_logs(0);  // records in a second ResourceLogs continue the count
    std::vector<LogRow> rows(1);                // rows from earlier messages do not
    transformLogs(request, {}, rows);
    ASSERT_EQ(rows.size(), 3u);
    EXPECT_EQ(rows[1].record_index, 0);
    EXPECT_EQ(rows[2].record_index, 1);
}

TEST(LogTransformerTest, AppendsToExistingRows) {
    std::vector<LogRow> rows(2);
    transformLogs(sampleRequest(), {}, rows);
    EXPECT_EQ(rows.size(), 3u);
}

TEST(LogTransformerTest, FallsBackToObservedTimeThenNull) {
    auto request = sampleRequest();
    auto* record = request.mutable_resource_logs(0)->mutable_scope_logs(0)->mutable_log_records(0);
    record->set_time_unix_nano(0);
    std::vector<LogRow> rows;
    transformLogs(request, {}, rows);
    EXPECT_EQ(rows[0].timestamp_us, 1'700'000'001'000'000LL);

    record->set_observed_time_unix_nano(0);
    rows.clear();
    transformLogs(request, {}, rows);
    EXPECT_FALSE(rows[0].timestamp_us.has_value());  // deterministic, never now()
}

TEST(LogTransformerTest, LegacyDeploymentEnvironmentKey) {
    ExportLogsServiceRequest request;
    auto* resource_logs = request.add_resource_logs();
    addAttr(resource_logs->mutable_resource()->mutable_attributes(), "deployment.environment",
            "staging");
    resource_logs->add_scope_logs()->add_log_records();
    std::vector<LogRow> rows;
    transformLogs(request, {}, rows);
    EXPECT_EQ(rows[0].resource->deployment_environment, "staging");
}

TEST(LogTransformerTest, SeverityTextPreferredOverNumber) {
    auto request = sampleRequest();
    request.mutable_resource_logs(0)->mutable_scope_logs(0)->mutable_log_records(0)
        ->set_severity_text("Warning");
    std::vector<LogRow> rows;
    transformLogs(request, {}, rows);
    EXPECT_EQ(rows[0].severity_text, "Warning");
    EXPECT_EQ(rows[0].severity_number, 14);
}

TEST(LogTransformerTest, SeverityNames) {
    EXPECT_EQ(severityName(0), "");
    EXPECT_EQ(severityName(1), "TRACE");
    EXPECT_EQ(severityName(4), "TRACE");
    EXPECT_EQ(severityName(5), "DEBUG");
    EXPECT_EQ(severityName(9), "INFO");
    EXPECT_EQ(severityName(17), "ERROR");
    EXPECT_EQ(severityName(24), "FATAL");
    EXPECT_EQ(severityName(25), "");
}

TEST(LogTransformerTest, AnyValueRendering) {
    AnyValue v;
    v.set_bool_value(true);
    EXPECT_EQ(anyValueToString(v), "true");
    v.set_int_value(-12);
    EXPECT_EQ(anyValueToString(v), "-12");
    v.set_double_value(0.1);
    EXPECT_EQ(anyValueToString(v), "0.1");
    v.set_bytes_value(std::string("\x00\xff", 2));
    EXPECT_EQ(anyValueToString(v), "00ff");

    AnyValue structured;
    auto* kv = structured.mutable_kvlist_value()->add_values();
    kv->set_key("user");
    auto* array = kv->mutable_value()->mutable_array_value();
    array->add_values()->set_string_value("a\"b");
    array->add_values()->set_int_value(std::numeric_limits<std::int64_t>::max());
    array->add_values();  // unset value -> null
    EXPECT_EQ(anyValueToString(structured), R"({"user":["a\"b",9223372036854775807,null]})");

    AnyValue empty;
    EXPECT_EQ(anyValueToString(empty), "");
}

TEST(OtlpDecoderTest, DecodesProtobufPayload) {
    const auto payload = wrap("application/x-protobuf", sampleRequest().SerializeAsString());
    auto decoded = decodeKafkaPayload(payload);
    ASSERT_TRUE(decoded) << decoded.error().message;
    EXPECT_EQ(decoded->resource_logs(0).scope_logs(0).log_records(0).event_name(),
              "checkout.completed");
}

TEST(OtlpDecoderTest, DecodesJsonWithHexIdsAndUnknownFields) {
    const std::string json = R"({
      "resourceLogs": [{
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "api"}}]},
        "futureField": {"x": 1},
        "scopeLogs": [{
          "scope": {"name": "s"},
          "logRecords": [{
            "timeUnixNano": "1700000000123456789",
            "observedTimeUnixNano": 1700000001000000000,
            "severityNumber": 9,
            "body": {"stringValue": "hello"},
            "traceId": "5B8EFFF798038103D269B633813FC60C",
            "spanId": "eee19b7ec3c1b174",
            "someNewField": true
          }]
        }]
      }]
    })";
    auto decoded = decodeKafkaPayload(wrap("application/json", json));
    ASSERT_TRUE(decoded) << decoded.error().message;

    std::vector<LogRow> rows;
    transformLogs(*decoded, {}, rows);
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].trace_id, "5b8efff798038103d269b633813fc60c");
    EXPECT_EQ(rows[0].span_id, "eee19b7ec3c1b174");
    EXPECT_EQ(rows[0].time_unix_nano, 1'700'000'000'123'456'789ULL);
    EXPECT_EQ(rows[0].observed_time_unix_nano, 1'700'000'001'000'000'000ULL);  // exact int64
    EXPECT_EQ(rows[0].severity_text, "INFO");
    EXPECT_EQ(rows[0].resource->service_name, "api");
}

TEST(OtlpDecoderTest, PoisonInputs) {
    auto expectPoison = [](const Result<ExportLogsServiceRequest>& r) {
        ASSERT_FALSE(r);
        EXPECT_EQ(r.error().kind, ErrorKind::Poison);
    };
    expectPoison(decodeKafkaPayload("\xff\xff\xff garbage"));
    expectPoison(decodeKafkaPayload(wrap("text/plain", "hi")));
    expectPoison(decodeKafkaPayload(wrap("application/json", "{not json")));
    expectPoison(decodeKafkaPayload(wrap("application/json", R"({"resourceLogs": 5})")));
    expectPoison(decodeKafkaPayload(wrap("application/x-protobuf", "\x0a\xff\xff\xff")));

    telemetry::v1::RawTelemetryMessage wrong_type;
    wrong_type.set_content_type("application/json");
    wrong_type.set_payload("{}");
    expectPoison(decodeKafkaPayload(wrong_type.SerializeAsString()));
}

TEST(OtlpDecoderTest, HexToBase64) {
    EXPECT_EQ(hexToBase64("00"), "AA==");
    EXPECT_EQ(hexToBase64("0001"), "AAE=");
    EXPECT_EQ(hexToBase64("000102"), "AAEC");
    EXPECT_EQ(hexToBase64("FFfe"), "//4=");
    EXPECT_EQ(hexToBase64(""), "");
    EXPECT_EQ(hexToBase64("abc"), "");     // odd length
    EXPECT_EQ(hexToBase64("zz"), "");      // not hex
}
