#ifndef APPENDER_STAGING_STORE_HPP
#define APPENDER_STAGING_STORE_HPP

#include "error.hpp"
#include "log_row.hpp"
#include "sealed_buffer.hpp"
#include "schema.hpp"
#include "sink.hpp"

#include "duckdb_include.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <vector>

namespace appender {

// Bytes held in staging (unsealed and sealed-but-uncommitted). Pipelines
// wait while it is over the cap, which in turn fills their queues and makes
// the Kafka source pause partitions.
class ByteBudget {
public:
    explicit ByteBudget(std::size_t cap) : cap_(cap) {}

    void add(std::size_t bytes);
    void release(std::size_t bytes);
    std::size_t used() const;
    std::size_t cap() const { return cap_; }

    // True once usage is below the cap; false on timeout
    bool waitBelowCap(std::chrono::milliseconds timeout);

private:
    const std::size_t cap_;
    mutable std::mutex mutex_;
    std::condition_variable below_cap_;
    std::size_t used_ = 0;
};

// Rows of one Kafka partition waiting for the next commit. Appends go into
// the current generation table; seal() hands that table to the committer and
// starts a new one, so appends never wait for an Iceberg commit.
class PartitionStaging : public PartitionWriter {
public:
    PartitionStaging(duckdb::DuckDB& db, std::int32_t partition, ByteBudget& budget,
                     std::shared_ptr<const schema::TableSchema> schema);
    ~PartitionStaging();

    PartitionStaging(const PartitionStaging&) = delete;
    PartitionStaging& operator=(const PartitionStaging&) = delete;

    Result<void> init();

    // Appends rows decoded from Kafka messages up to max_offset; bytes is the
    // payload size accounted against the budget. All-or-nothing.
    Result<void> append(std::span<const LogRow> rows, std::int64_t max_offset,
                        std::size_t bytes, std::int64_t kafka_ts_ms = -1) override;

    // Advances the offset without rows (e.g. a message with no log records,
    // or a poison message already sent to the DLQ)
    void advanceOffset(std::int64_t offset) override;

    // nullopt when nothing new was staged since the last seal
    Result<std::optional<SealedBuffer>> seal();

    // Drops everything not yet sealed
    void discard();

    std::int32_t partition() const { return partition_; }
    std::size_t bytes() const;
    std::optional<std::chrono::steady_clock::time_point> oldestRow() const;

private:
    Result<void> createGeneration();

    const std::int32_t partition_;
    ByteBudget& budget_;
    const std::shared_ptr<const schema::TableSchema> schema_;
    mutable std::mutex mutex_;
    duckdb::Connection conn_;
    std::uint64_t generation_ = 0;
    std::string table_;
    std::size_t rows_ = 0;
    std::size_t bytes_ = 0;
    std::int64_t max_offset_ = -1;
    bool dirty_ = false;  // offset advanced since the last seal
    std::int64_t min_kafka_ts_ms_ = -1;
    std::int64_t max_kafka_ts_ms_ = -1;
    std::optional<std::chrono::steady_clock::time_point> first_append_;
};

// The staging buffers of all partitions currently assigned to this process
class StagingRegistry : public StagingSource, public StagingLookup {
public:
    StagingRegistry(duckdb::DuckDB& db, std::size_t max_staged_bytes,
                    std::shared_ptr<const schema::TableSchema> schema = std::make_shared<schema::TableSchema>());

    Result<void> add(std::int32_t partition);
    void remove(std::int32_t partition);  // discards unsealed rows
    std::shared_ptr<PartitionStaging> find(std::int32_t partition) const;
    std::shared_ptr<PartitionWriter> writer(std::int32_t partition) override { return find(partition); }
    bool waitForCapacity(std::chrono::milliseconds timeout) override {
        return budget_.waitBelowCap(timeout);
    }
    std::vector<std::int32_t> partitions() const;

    // Seals every partition not in excluded that has something staged
    Result<std::vector<SealedBuffer>> sealAll(const std::set<std::int32_t>& excluded = {}) override;

    // Drops a sealed table (after its commit, or when it is abandoned)
    void dropSealed(const SealedBuffer& sealed) override;

    // Drops unsealed rows of these partitions (used on revoke)
    void discard(const std::set<std::int32_t>& partitions) override;

    ByteBudget& budget() { return budget_; }
    std::size_t unsealedBytes() const override;
    std::optional<std::chrono::steady_clock::time_point> oldestUnsealedRow() const override;

private:
    duckdb::DuckDB& db_;
    ByteBudget budget_;
    const std::shared_ptr<const schema::TableSchema> schema_;
    mutable std::mutex mutex_;
    std::map<std::int32_t, std::shared_ptr<PartitionStaging>> partitions_;
    std::mutex drop_mutex_;
    duckdb::Connection drop_conn_;
};

}  // namespace appender

#endif  // APPENDER_STAGING_STORE_HPP
