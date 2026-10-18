#ifndef APPENDER_LOG_TRANSFORMER_HPP
#define APPENDER_LOG_TRANSFORMER_HPP

#include "log_row.hpp"
#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace appender {

struct KafkaCoordinates {
    std::int32_t partition = 0;
    std::int64_t offset = 0;
};

// Flattens every log record of request into out (appending). Pure and
// deterministic: the same input always yields the same rows.
void transformLogs(const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request,
                   KafkaCoordinates source, std::vector<LogRow>& out);

// Severity text for an OTLP severity number ("" for 0 / out of range)
std::string_view severityName(int severity_number);

// Attribute and body values: strings verbatim, scalars in their natural
// text form, bytes as hex, arrays and maps as compact JSON
std::string anyValueToString(const opentelemetry::proto::common::v1::AnyValue& value);

std::string bytesToHex(std::string_view bytes);

}  // namespace appender

#endif  // APPENDER_LOG_TRANSFORMER_HPP
