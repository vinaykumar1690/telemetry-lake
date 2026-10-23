#ifndef APPENDER_SINK_HPP
#define APPENDER_SINK_HPP

#include "error.hpp"
#include "log_row.hpp"
#include "sealed_buffer.hpp"

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
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

// Where pipelines put decoded rows of one partition (PartitionStaging)
class PartitionWriter {
public:
    virtual ~PartitionWriter() = default;
    // Stages rows (possibly none) and moves the partition's offset to max_offset.
    // kafka_ts_ms is the message's Kafka timestamp (-1 if unknown).
    virtual Result<void> append(std::span<const LogRow> rows, std::int64_t max_offset,
                                std::size_t bytes, std::int64_t kafka_ts_ms = -1) = 0;
    // Moves the offset without rows (a poison message that reached the DLQ)
    virtual void advanceOffset(std::int64_t offset) = 0;
};

// Pipelines' view of the staging area (StagingRegistry)
class StagingLookup {
public:
    virtual ~StagingLookup() = default;
    // nullptr when the partition is not (or no longer) assigned
    virtual std::shared_ptr<PartitionWriter> writer(std::int32_t partition) = 0;
    // Blocks while staged bytes are over the cap; false on timeout
    virtual bool waitForCapacity(std::chrono::milliseconds timeout) = 0;
};

struct CommitContext {
    std::chrono::system_clock::time_point ingested_at;
};

// What the last commit added to the logs table, from its snapshot summary
struct CommitFiles {
    std::uint64_t added_data_files = 0;
    std::optional<std::uint64_t> total_data_files;
};

// Writes sealed buffers to the logs table and their offsets to the offsets
// table in one atomic Iceberg commit. Errors: Retryable when the commit
// certainly did not land, Indeterminate when it may have, Fatal otherwise.
class IcebergSink {
public:
    virtual ~IcebergSink() = default;
    virtual Result<void> commit(std::span<const SealedBuffer> buffers, const CommitContext& context) = 0;

    // Data files of the commit that just succeeded with this many rows; nullopt
    // when unknown (e.g. another writer committed since). Best effort.
    virtual std::optional<CommitFiles> lastCommitFiles(std::size_t /*rows*/) { return std::nullopt; }
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
