#ifndef APPENDER_DUCKDB_ICEBERG_HPP
#define APPENDER_DUCKDB_ICEBERG_HPP

#include "duckdb_session.hpp"
#include "sink.hpp"

#include <memory>
#include <optional>
#include <string>

namespace appender {

struct SinkIdentity {
    std::string topic;
    std::string consumer_group;
    std::string appender_id;
};

// Commits sealed staging tables with one DuckDB transaction that inserts into
// both the logs and the offsets table. DuckDB sends Iceberg commits touching
// several tables as a single REST transactions/commit, so the rows and the
// offsets that describe them become visible together or not at all.
class DuckdbIcebergSink : public IcebergSink {
public:
    DuckdbIcebergSink(DuckdbSession& session, TableNames tables, SinkIdentity identity);

    Result<void> commit(std::span<const SealedBuffer> buffers, const CommitContext& context) override;

    // Reads the logs table's current snapshot summary from the catalog (one
    // loadTable request)
    std::optional<CommitFiles> lastCommitFiles(std::size_t rows) override;

private:
    std::unique_ptr<duckdb::Connection> conn_;
    TableNames tables_;
    std::shared_ptr<const schema::TableSchema> schema_;
    SinkIdentity identity_;
};

// Reads the offsets table. Recovery depends only on the topic: whichever
// consumer group or replica wrote a row, its data is in the logs table.
class DuckdbOffsetStore : public OffsetStore {
public:
    DuckdbOffsetStore(DuckdbSession& session, std::string offsets_table, std::string topic);

    Result<std::map<std::int32_t, std::int64_t>> lastCommitted(
        std::span<const std::int32_t> partitions) override;
    Result<void> prune() override;

private:
    std::unique_ptr<duckdb::Connection> conn_;
    std::string offsets_table_;
    std::string topic_;
    std::optional<bool> rewrite_available_;
};

// Files of the current snapshot in an Iceberg table-metadata document, if
// that snapshot is an append of exactly `rows` rows (i.e. ours); exposed for tests
std::optional<CommitFiles> commitFilesFromMetadata(std::string_view metadata_json, std::size_t rows);

// SQL text for one commit; exposed for tests
std::string buildCommitSql(std::span<const SealedBuffer> buffers, const TableNames& tables,
                           const schema::TableSchema& schema, const SinkIdentity& identity,
                           const CommitContext& context);

}  // namespace appender

#endif  // APPENDER_DUCKDB_ICEBERG_HPP
