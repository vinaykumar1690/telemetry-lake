#include "otlp_decoder.hpp"

#include "telemetry_wrapper.pb.h"

#include <google/protobuf/util/json_util.h>
#include <nlohmann/json.hpp>

#include <format>
#include <initializer_list>
#include <optional>

namespace appender {
namespace {

std::optional<std::uint8_t> hexDigit(char c) {
    if (c >= '0' && c <= '9') return static_cast<std::uint8_t>(c - '0');
    if (c >= 'a' && c <= 'f') return static_cast<std::uint8_t>(c - 'a' + 10);
    if (c >= 'A' && c <= 'F') return static_cast<std::uint8_t>(c - 'A' + 10);
    return std::nullopt;
}

std::string base64(std::string_view bytes) {
    static constexpr std::string_view kAlphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve((bytes.size() + 2) / 3 * 4);
    std::size_t i = 0;
    for (; i + 3 <= bytes.size(); i += 3) {
        const auto n = (std::uint32_t(std::uint8_t(bytes[i])) << 16) |
                       (std::uint32_t(std::uint8_t(bytes[i + 1])) << 8) |
                       std::uint32_t(std::uint8_t(bytes[i + 2]));
        out += kAlphabet[(n >> 18) & 63];
        out += kAlphabet[(n >> 12) & 63];
        out += kAlphabet[(n >> 6) & 63];
        out += kAlphabet[n & 63];
    }
    if (const auto rest = bytes.size() - i; rest > 0) {
        auto n = std::uint32_t(std::uint8_t(bytes[i])) << 16;
        if (rest == 2) n |= std::uint32_t(std::uint8_t(bytes[i + 1])) << 8;
        out += kAlphabet[(n >> 18) & 63];
        out += kAlphabet[(n >> 12) & 63];
        out += rest == 2 ? kAlphabet[(n >> 6) & 63] : '=';
        out += '=';
    }
    return out;
}

// Rewrites hex trace/span IDs of every log record in place
void rewriteHexIds(nlohmann::json& document) {
    auto children = [](nlohmann::json& parent,
                       std::initializer_list<const char*> names) -> nlohmann::json* {
        if (!parent.is_object()) return nullptr;
        for (const char* name : names) {
            if (auto it = parent.find(name); it != parent.end() && it->is_array()) return &*it;
        }
        return nullptr;
    };
    auto fix = [](nlohmann::json& record, std::initializer_list<const char*> names) {
        for (const char* name : names) {
            auto it = record.find(name);
            if (it == record.end() || !it->is_string()) continue;
            // Values that are not hex (e.g. a sender already using base64) are left alone
            if (auto encoded = hexToBase64(it->get_ref<const std::string&>()); !encoded.empty()) {
                *it = std::move(encoded);
            }
        }
    };

    auto* resource_logs = children(document, {"resourceLogs", "resource_logs"});
    if (!resource_logs) return;
    for (auto& resource : *resource_logs) {
        auto* scope_logs = children(resource, {"scopeLogs", "scope_logs"});
        if (!scope_logs) continue;
        for (auto& scope : *scope_logs) {
            auto* records = children(scope, {"logRecords", "log_records"});
            if (!records) continue;
            for (auto& record : *records) {
                if (!record.is_object()) continue;
                fix(record, {"traceId", "trace_id"});
                fix(record, {"spanId", "span_id"});
            }
        }
    }
}

}  // namespace

std::string hexToBase64(std::string_view hex) {
    if (hex.empty() || hex.size() % 2 != 0) return {};
    std::string bytes;
    bytes.reserve(hex.size() / 2);
    for (std::size_t i = 0; i < hex.size(); i += 2) {
        const auto hi = hexDigit(hex[i]);
        const auto lo = hexDigit(hex[i + 1]);
        if (!hi || !lo) return {};
        bytes += static_cast<char>((*hi << 4) | *lo);
    }
    return base64(bytes);
}

Result<ExportLogsServiceRequest> decodeOtlpJson(std::string_view json) {
    auto document = nlohmann::json::parse(json, nullptr, /*allow_exceptions=*/false);
    if (document.is_discarded()) {
        return poison("payload is not valid JSON");
    }
    rewriteHexIds(document);

    google::protobuf::util::JsonParseOptions options;
    options.ignore_unknown_fields = true;
    ExportLogsServiceRequest request;
    const auto status =
        google::protobuf::util::JsonStringToMessage(document.dump(), &request, options);
    if (!status.ok()) {
        return poison(std::format("invalid OTLP/JSON: {}", status.ToString()));
    }
    return request;
}

Result<ExportLogsServiceRequest> decodeKafkaPayload(std::string_view payload) {
    telemetry::v1::RawTelemetryMessage wrapper;
    if (!wrapper.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
        return poison("not a RawTelemetryMessage");
    }
    if (wrapper.telemetry_type() != telemetry::v1::OTEL_LOGS) {
        return poison(std::format("unsupported telemetry type {}",
                                  static_cast<int>(wrapper.telemetry_type())));
    }

    // The ingester writes the canonical lowercase media type
    const std::string& content_type = wrapper.content_type();
    if (content_type == "application/x-protobuf" || content_type == "application/protobuf") {
        ExportLogsServiceRequest request;
        if (!request.ParseFromString(wrapper.payload())) {
            return poison("invalid OTLP/protobuf payload");
        }
        return request;
    }
    if (content_type == "application/json" || content_type == "text/json") {
        return decodeOtlpJson(wrapper.payload());
    }
    return poison(std::format("unsupported content type '{}'", content_type));
}

}  // namespace appender
