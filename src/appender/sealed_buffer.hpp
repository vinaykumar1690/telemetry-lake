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
};

}  // namespace appender

#endif  // APPENDER_SEALED_BUFFER_HPP
