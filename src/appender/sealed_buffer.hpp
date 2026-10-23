#ifndef APPENDER_SEALED_BUFFER_HPP
#define APPENDER_SEALED_BUFFER_HPP

#include <cstddef>
#include <cstdint>
#include <string>

namespace appender {

// A staging generation handed from a partition to the committer. rows may be
// 0 when only the offset moved (poison or empty messages).
struct SealedBuffer {
    std::int32_t partition = 0;
    std::string table;
    std::int64_t max_offset = -1;
    std::size_t rows = 0;
    std::size_t bytes = 0;
    // Kafka message timestamps (ms since the epoch) of the staged messages;
    // -1 when none had one
    std::int64_t min_kafka_ts_ms = -1;
    std::int64_t max_kafka_ts_ms = -1;
};

}  // namespace appender

#endif  // APPENDER_SEALED_BUFFER_HPP
