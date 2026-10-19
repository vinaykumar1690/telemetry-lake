#include "log_transformer.hpp"

#include <nlohmann/json.hpp>

#include <array>
#include <cmath>
#include <format>
#include <limits>

namespace appender {
namespace {

using opentelemetry::proto::common::v1::AnyValue;
using opentelemetry::proto::common::v1::KeyValue;

nlohmann::json anyValueToJson(const AnyValue& value) {
    switch (value.value_case()) {
        case AnyValue::kStringValue: return value.string_value();
        case AnyValue::kBoolValue: return value.bool_value();
        case AnyValue::kIntValue: return value.int_value();
        case AnyValue::kDoubleValue: {
            const double d = value.double_value();
            // JSON has no NaN/Infinity literals
            if (!std::isfinite(d)) return std::format("{}", d);
            return d;
        }
        case AnyValue::kBytesValue: return bytesToHex(value.bytes_value());
        case AnyValue::kArrayValue: {
            auto array = nlohmann::json::array();
            for (const auto& item : value.array_value().values()) {
                array.push_back(anyValueToJson(item));
            }
            return array;
        }
        case AnyValue::kKvlistValue: {
            auto object = nlohmann::json::object();
            for (const auto& kv : value.kvlist_value().values()) {
                object[kv.key()] = anyValueToJson(kv.value());
            }
            return object;
        }
        case AnyValue::VALUE_NOT_SET: break;
    }
    return nullptr;
}

Attributes toAttributes(const google::protobuf::RepeatedPtrField<KeyValue>& kvs) {
    Attributes out;
    out.reserve(kvs.size());
    for (const auto& kv : kvs) {
        out.emplace_back(kv.key(), anyValueToString(kv.value()));
    }
    return out;
}

std::string_view lookup(const Attributes& attributes, std::string_view key) {
    for (const auto& [k, v] : attributes) {
        if (k == key) return v;
    }
    return {};
}

std::shared_ptr<const ResourceFields> resourceFields(
    const opentelemetry::proto::resource::v1::Resource& resource) {
    auto fields = std::make_shared<ResourceFields>();
    fields->attributes = toAttributes(resource.attributes());
    const auto& attrs = fields->attributes;
    fields->service_name = lookup(attrs, "service.name");
    fields->service_namespace = lookup(attrs, "service.namespace");
    fields->service_instance_id = lookup(attrs, "service.instance.id");
    // deployment.environment.name replaced deployment.environment in semconv 1.27
    fields->deployment_environment = lookup(attrs, "deployment.environment.name");
    if (fields->deployment_environment.empty()) {
        fields->deployment_environment = lookup(attrs, "deployment.environment");
    }
    fields->host_name = lookup(attrs, "host.name");
    return fields;
}

std::optional<std::int64_t> toMicros(std::uint64_t nanos) {
    if (nanos == 0) return std::nullopt;
    return static_cast<std::int64_t>(nanos / 1000);
}

}  // namespace

std::string_view severityName(int severity_number) {
    // Severity numbers come in groups of four per level (1-4 TRACE, ..., 21-24 FATAL)
    static constexpr std::array<std::string_view, 6> kLevels{"TRACE", "DEBUG", "INFO",
                                                            "WARN",  "ERROR", "FATAL"};
    if (severity_number < 1 || severity_number > 24) return {};
    return kLevels[static_cast<std::size_t>((severity_number - 1) / 4)];
}

std::string bytesToHex(std::string_view bytes) {
    static constexpr std::string_view kDigits = "0123456789abcdef";
    std::string out;
    out.reserve(bytes.size() * 2);
    for (unsigned char c : bytes) {
        out += kDigits[c >> 4];
        out += kDigits[c & 0x0f];
    }
    return out;
}

std::string anyValueToString(const AnyValue& value) {
    switch (value.value_case()) {
        case AnyValue::kStringValue: return value.string_value();
        case AnyValue::kBoolValue: return value.bool_value() ? "true" : "false";
        case AnyValue::kIntValue: return std::format("{}", value.int_value());
        case AnyValue::kDoubleValue: return std::format("{}", value.double_value());
        case AnyValue::kBytesValue: return bytesToHex(value.bytes_value());
        case AnyValue::kArrayValue:
        case AnyValue::kKvlistValue:
            // Replace invalid UTF-8 instead of throwing; the rest is preserved
            return anyValueToJson(value).dump(-1, ' ', false,
                                              nlohmann::json::error_handler_t::replace);
        case AnyValue::VALUE_NOT_SET: break;
    }
    return {};
}

void transformLogs(const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request,
                   KafkaCoordinates source, std::vector<LogRow>& out) {
    std::int32_t record_index = 0;
    for (const auto& resource_logs : request.resource_logs()) {
        const auto resource = resourceFields(resource_logs.resource());
        for (const auto& scope_logs : resource_logs.scope_logs()) {
            auto scope = std::make_shared<ScopeFields>();
            scope->name = scope_logs.scope().name();
            scope->version = scope_logs.scope().version();

            for (const auto& record : scope_logs.log_records()) {
                LogRow& row = out.emplace_back();
                row.kafka_partition = source.partition;
                row.kafka_offset = source.offset;
                row.record_index = record_index++;

                row.time_unix_nano = record.time_unix_nano();
                row.observed_time_unix_nano = record.observed_time_unix_nano();
                row.timestamp_us = toMicros(record.time_unix_nano());
                if (!row.timestamp_us) row.timestamp_us = toMicros(record.observed_time_unix_nano());

                row.severity_number = static_cast<std::int32_t>(record.severity_number());
                row.severity_text = record.severity_text().empty()
                                        ? std::string(severityName(row.severity_number))
                                        : record.severity_text();
                if (record.has_body()) row.body = anyValueToString(record.body());
                row.trace_id = bytesToHex(record.trace_id());
                row.span_id = bytesToHex(record.span_id());
                row.flags = record.flags();
                row.event_name = record.event_name();

                row.resource = resource;
                row.scope = scope;
                row.attributes = toAttributes(record.attributes());
                row.dropped_attributes_count = record.dropped_attributes_count();
            }
        }
    }
}

}  // namespace appender
