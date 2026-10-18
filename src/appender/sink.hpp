#ifndef APPENDER_SINK_HPP
#define APPENDER_SINK_HPP

#include "error.hpp"
#include "sealed_buffer.hpp"

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <vector>

// Seams between the committer and DuckDB/Iceberg, so the commit protocol can
// be tested with fakes

namespace appender {

// Where the committer takes its batches from (StagingRegistry in production)
class StagingSource {
public:
    virtual ~StagingSource() = default;
    virtual Result<std::vector<SealedBuffer>> sealAll(const std::set<std::int32_t>& excluded) = 0;
    virtual void dropSealed(const SealedBuffer& sealed) = 0;
    virtual void discard(const std::set<std::int32_t>& partitions) = 0;
    virtual std::size_t unsealedBytes() const = 0;
    virtual std::optional<std::chrono::steady_clock::time_point> oldestUnsealedRow() const = 0;
};

struct CommitContext {
    std::chrono::system_clock::time_point ingested_at;
};

// Writes sealed buffers to the logs table and their offsets to the offsets
// table in one atomic Iceberg commit. Errors: Retryable when the commit
// certainly did not land, Indeterminate when it may have, Fatal otherwise.
class IcebergSink {
public:
    virtual ~IcebergSink() = default;
    virtual Result<void> commit(std::span<const SealedBuffer> buffers, const CommitContext& context) = 0;
};

// Reads back what the offsets table says is durable
class OffsetStore {
public:
    virtual ~OffsetStore() = default;
    // Last durable offset per partition; partitions never committed are absent
    virtual Result<std::map<std::int32_t, std::int64_t>> lastCommitted(
        std::span<const std::int32_t> partitions) = 0;
    // Removes superseded offsets rows; best effort
    virtual Result<void> prune() = 0;
};

}  // namespace appender

#endif  // APPENDER_SINK_HPP
