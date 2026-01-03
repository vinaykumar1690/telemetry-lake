#ifndef LOG_TRANSFORMER_HPP
#define LOG_TRANSFORMER_HPP

#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"
#include <string>
#include <map>
#include <vector>
#include <chrono>

// Represents a single transformed log record ready for insertion into Iceberg
struct TransformedLogRecord {
    std::chrono::system_clock::time_point timestamp;
    std::string severity;
    std::string body;
    std::string trace_id;  // hex-encoded
    std::string span_id;   // hex-encoded
    std::string service_name;
    std::string deployment_environment;
    std::string host_name;
    std::map<std::string, std::string> attributes;  // All other attributes
};

class LogTransformer {
public:
    // Transform an ExportLogsServiceRequest into a vector of TransformedLogRecord
    // Each log record in the request becomes one TransformedLogRecord
    static std::vector<TransformedLogRecord> transform(
        const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request);

private:
    // Extract a string value from an AnyValue
    static std::string extractStringValue(const opentelemetry::proto::common::v1::AnyValue& value);
    
    // Extract attribute value as string
    static std::string extractAttributeValue(const opentelemetry::proto::common::v1::KeyValue& kv);
    
    // Extract well-known attributes from resource
    static void extractWellKnownAttributes(
        const opentelemetry::proto::resource::v1::Resource& resource,
        std::string& service_name,
        std::string& deployment_environment,
        std::string& host_name);
    
    // Convert nanoseconds since epoch to time_point
    static std::chrono::system_clock::time_point nanosToTimePoint(uint64_t nanos);
    
    // Convert bytes to hex string
    static std::string bytesToHex(const std::string& bytes);
    
    // Convert severity number to text if severity_text is empty
    static std::string getSeverityText(
        const opentelemetry::proto::logs::v1::LogRecord& log_record);
};

#endif // LOG_TRANSFORMER_HPP

