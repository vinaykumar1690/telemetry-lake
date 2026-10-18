#ifndef APPENDER_OTLP_DECODER_HPP
#define APPENDER_OTLP_DECODER_HPP

#include "error.hpp"
#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"

#include <string>
#include <string_view>

namespace appender {

using opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;

// Decodes a Kafka message produced by the ingester (a RawTelemetryMessage
// wrapping an OTLP/HTTP body). Every failure is reported as Poison: the bytes
// will never decode, so retrying cannot help.
Result<ExportLogsServiceRequest> decodeKafkaPayload(std::string_view payload);

// Parses an OTLP/JSON body. OTLP/JSON encodes traceId/spanId as hex where
// protobuf's JSON mapping expects base64, so those fields are rewritten first.
// Unknown fields are ignored for forward compatibility.
Result<ExportLogsServiceRequest> decodeOtlpJson(std::string_view json);

// Lowercase-hex text to base64; empty string when the input is not hex
std::string hexToBase64(std::string_view hex);

}  // namespace appender

#endif  // APPENDER_OTLP_DECODER_HPP
