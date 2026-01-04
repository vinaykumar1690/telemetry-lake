#include "log_transformer.hpp"
#include "opentelemetry/proto/common/v1/common.pb.h"
#include "opentelemetry/proto/resource/v1/resource.pb.h"
#include "opentelemetry/proto/logs/v1/logs.pb.h"
#include <sstream>
#include <iomanip>
#include <algorithm>

std::vector<TransformedLogRecord> LogTransformer::transform(
    const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request) {
    
    std::vector<TransformedLogRecord> records;
    
    // Iterate through all resource logs
    for (int i = 0; i < request.resource_logs_size(); ++i) {
        const auto& resource_logs = request.resource_logs(i);
        const auto& resource = resource_logs.resource();
        
        // Extract well-known attributes from resource
        std::string service_name;
        std::string deployment_environment;
        std::string host_name;
        extractWellKnownAttributes(resource, service_name, deployment_environment, host_name);
        
        // Iterate through all scope logs
        for (int j = 0; j < resource_logs.scope_logs_size(); ++j) {
            const auto& scope_logs = resource_logs.scope_logs(j);
            
            // Iterate through all log records
            for (int k = 0; k < scope_logs.log_records_size(); ++k) {
                const auto& log_record = scope_logs.log_records(k);
                
                TransformedLogRecord transformed;
                
                // Extract timestamp
                if (log_record.time_unix_nano() > 0) {
                    transformed.timestamp = nanosToTimePoint(log_record.time_unix_nano());
                } else {
                    // Use observed time if time_unix_nano is not set
                    transformed.timestamp = nanosToTimePoint(log_record.observed_time_unix_nano());
                }
                
                // Extract severity
                transformed.severity = getSeverityText(log_record);
                
                // Extract body
                if (log_record.has_body()) {
                    transformed.body = extractStringValue(log_record.body());
                }
                
                // Extract trace_id and span_id
                if (log_record.trace_id().size() > 0) {
                    transformed.trace_id = bytesToHex(log_record.trace_id());
                }
                if (log_record.span_id().size() > 0) {
                    transformed.span_id = bytesToHex(log_record.span_id());
                }
                
                // Set well-known attributes
                transformed.service_name = service_name;
                transformed.deployment_environment = deployment_environment;
                transformed.host_name = host_name;
                
                // Extract all attributes (from both resource and log record)
                // Well-known ones are already extracted, so we'll skip them
                std::map<std::string, std::string> all_attributes;
                
                // Add resource attributes (except well-known ones)
                for (int attr_idx = 0; attr_idx < resource.attributes_size(); ++attr_idx) {
                    const auto& attr = resource.attributes(attr_idx);
                    std::string key = attr.key();
                    
                    // Skip well-known attributes
                    if (key != "service.name" && 
                        key != "deployment.environment" && 
                        key != "host.name") {
                        all_attributes[key] = extractAttributeValue(attr);
                    }
                }
                
                // Add log record attributes
                for (int attr_idx = 0; attr_idx < log_record.attributes_size(); ++attr_idx) {
                    const auto& attr = log_record.attributes(attr_idx);
                    all_attributes[attr.key()] = extractAttributeValue(attr);
                }
                
                transformed.attributes = all_attributes;
                
                records.push_back(transformed);
            }
        }
    }
    
    return records;
}

std::string LogTransformer::extractStringValue(const opentelemetry::proto::common::v1::AnyValue& value) {
    switch (value.value_case()) {
        case opentelemetry::proto::common::v1::AnyValue::kStringValue:
            return value.string_value();
        case opentelemetry::proto::common::v1::AnyValue::kBoolValue:
            return value.bool_value() ? "true" : "false";
        case opentelemetry::proto::common::v1::AnyValue::kIntValue:
            return std::to_string(value.int_value());
        case opentelemetry::proto::common::v1::AnyValue::kDoubleValue:
            return std::to_string(value.double_value());
        case opentelemetry::proto::common::v1::AnyValue::kBytesValue:
            return bytesToHex(value.bytes_value());
        case opentelemetry::proto::common::v1::AnyValue::kArrayValue: {
            std::ostringstream oss;
            const auto& array = value.array_value();
            for (int i = 0; i < array.values_size(); ++i) {
                if (i > 0) oss << ",";
                oss << extractStringValue(array.values(i));
            }
            return oss.str();
        }
        case opentelemetry::proto::common::v1::AnyValue::kKvlistValue: {
            std::ostringstream oss;
            const auto& kvlist = value.kvlist_value();
            for (int i = 0; i < kvlist.values_size(); ++i) {
                if (i > 0) oss << ",";
                const auto& kv = kvlist.values(i);
                oss << kv.key() << "=" << extractStringValue(kv.value());
            }
            return oss.str();
        }
        default:
            return "";
    }
}

std::string LogTransformer::extractAttributeValue(const opentelemetry::proto::common::v1::KeyValue& kv) {
    if (kv.has_value()) {
        return extractStringValue(kv.value());
    }
    return "";
}

void LogTransformer::extractWellKnownAttributes(
    const opentelemetry::proto::resource::v1::Resource& resource,
    std::string& service_name,
    std::string& deployment_environment,
    std::string& host_name) {
    
    for (int i = 0; i < resource.attributes_size(); ++i) {
        const auto& attr = resource.attributes(i);
        std::string key = attr.key();
        std::string value = extractAttributeValue(attr);
        
        if (key == "service.name") {
            service_name = value;
        } else if (key == "deployment.environment") {
            deployment_environment = value;
        } else if (key == "host.name") {
            host_name = value;
        }
    }
}

std::chrono::system_clock::time_point LogTransformer::nanosToTimePoint(uint64_t nanos) {
    if (nanos == 0) {
        return std::chrono::system_clock::now();
    }
    
    // Convert nanoseconds to system_clock::duration (which may be microseconds on some platforms)
    auto duration = std::chrono::duration_cast<std::chrono::system_clock::duration>(
        std::chrono::nanoseconds(nanos));
    
    // Create time_point from duration since epoch
    std::chrono::system_clock::time_point tp = 
        std::chrono::system_clock::time_point(duration);
    
    return tp;
}

std::string LogTransformer::bytesToHex(const std::string& bytes) {
    std::ostringstream oss;
    for (unsigned char c : bytes) {
        oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c);
    }
    return oss.str();
}

std::string LogTransformer::getSeverityText(const opentelemetry::proto::logs::v1::LogRecord& log_record) {
    // Prefer severity_text if available
    if (!log_record.severity_text().empty()) {
        return log_record.severity_text();
    }
    
    // Fall back to severity_number
    switch (log_record.severity_number()) {
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_TRACE:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_TRACE2:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_TRACE3:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_TRACE4:
            return "TRACE";
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_DEBUG:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_DEBUG2:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_DEBUG3:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_DEBUG4:
            return "DEBUG";
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_INFO:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_INFO2:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_INFO3:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_INFO4:
            return "INFO";
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_WARN:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_WARN2:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_WARN3:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_WARN4:
            return "WARN";
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_ERROR:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_ERROR2:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_ERROR3:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_ERROR4:
            return "ERROR";
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_FATAL:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_FATAL2:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_FATAL3:
        case opentelemetry::proto::logs::v1::SEVERITY_NUMBER_FATAL4:
            return "FATAL";
        default:
            return "UNSPECIFIED";
    }
}

