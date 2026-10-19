#ifndef APPENDER_DUCKDB_SESSION_HPP
#define APPENDER_DUCKDB_SESSION_HPP

#include "appender_config.hpp"
#include "error.hpp"
#include "schema.hpp"

#include "duckdb_include.hpp"

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace appender {

// Fully qualified names of the two destination tables
struct TableNames {
    std::string logs;
    std::string offsets;
};

// Runs a statement and maps a DuckDB error to Result
Result<std::unique_ptr<duckdb::MaterializedQueryResult>> query(duckdb::Connection& conn,
                                                               const std::string& sql);

// Classifies a DuckDB/Iceberg error message: conflicts, HTTP 5xx and network
// failures are retryable; anything else (types, schema, syntax) is fatal
ErrorKind classifyDuckdbError(std::string_view message);

// One in-memory DuckDB instance shared by the staging tables, the committer
// and recovery. Each user takes its own Connection.
class DuckdbSession {
public:
    // Loads httpfs + iceberg, registers the S3 secret, attaches the REST
    // catalog and creates or validates both tables
    static Result<std::unique_ptr<DuckdbSession>> openIceberg(const AppenderConfig& config);

    // Plain DuckDB with the two tables in the default schema; used by tests
    // in place of Iceberg
    static Result<std::unique_ptr<DuckdbSession>> openLocal(
        const TableNames& tables,
        std::shared_ptr<const schema::TableSchema> schema = std::make_shared<schema::TableSchema>());

    duckdb::DuckDB& database() { return *db_; }
    const TableNames& tables() const { return tables_; }
    const std::shared_ptr<const schema::TableSchema>& schema() const { return schema_; }
    std::unique_ptr<duckdb::Connection> connect() { return std::make_unique<duckdb::Connection>(*db_); }

private:
    DuckdbSession(TableNames tables, std::shared_ptr<const schema::TableSchema> schema);
    Result<void> pinUtc();

    std::unique_ptr<duckdb::DuckDB> db_;
    TableNames tables_;
    std::shared_ptr<const schema::TableSchema> schema_;
};

// CREATE TABLE IF NOT EXISTS with the given columns (and partition clause),
// then checks that the existing table has exactly those columns
Result<void> ensureTable(duckdb::Connection& conn, const std::string& qualified_name,
                         std::span<const schema::ColumnDef> columns,
                         std::string_view partition_clause = {});

// The logs table: like ensureTable for the base columns, which must come
// first and match exactly. After them, every column must be VARCHAR (a
// promoted attribute, current or former); configured promoted columns that
// are missing are added. Former ones stay and are written as NULL.
Result<void> ensureLogsTable(duckdb::Connection& conn, const std::string& qualified_name,
                             const schema::TableSchema& schema, std::string_view partition_clause = {});

std::string columnList(std::span<const schema::ColumnDef> columns);

}  // namespace appender

#endif  // APPENDER_DUCKDB_SESSION_HPP
