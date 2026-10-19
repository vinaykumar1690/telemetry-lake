#include "duckdb_session.hpp"

#include "sql.hpp"

#include <algorithm>
#include <array>
#include <format>
#include <iostream>

void std::default_delete<duckdb::ActiveTimer>::operator()(duckdb::ActiveTimer* timer) const noexcept {
    delete timer;
}

namespace appender {
namespace {

std::string_view partitionClause(PartitionSpec spec) {
    switch (spec) {
        case PartitionSpec::Day: return R"(PARTITIONED BY (day("timestamp")))";
        case PartitionSpec::Hour: return R"(PARTITIONED BY (hour("timestamp")))";
        case PartitionSpec::None: return {};
    }
    return {};
}

Result<void> run(duckdb::Connection& conn, const std::string& sql) {
    auto result = query(conn, sql);
    if (!result) return std::unexpected(result.error());
    return {};
}

}  // namespace

Result<std::unique_ptr<duckdb::MaterializedQueryResult>> query(duckdb::Connection& conn,
                                                               const std::string& sql) {
    try {
        auto result = conn.Query(sql);
        if (result->HasError()) {
            const std::string& message = result->GetError();
            return std::unexpected(AppenderError{classifyDuckdbError(message), message});
        }
        return result;
    } catch (const std::exception& e) {
        return std::unexpected(AppenderError{classifyDuckdbError(e.what()), e.what()});
    }
}

ErrorKind classifyDuckdbError(std::string_view message) {
    // Errors that mean the statement or the schema is wrong; retrying cannot fix them
    static constexpr std::array<std::string_view, 6> kFatal{
        "Parser Error", "Binder Error",   "Catalog Error",
        "Conversion Error", "Not implemented Error", "Permission Error"};
    for (auto prefix : kFatal) {
        if (message.find(prefix) != std::string_view::npos) return ErrorKind::Fatal;
    }
    return ErrorKind::Retryable;
}

std::string columnList(std::span<const schema::ColumnDef> columns) {
    std::string out;
    for (const auto& column : columns) {
        if (!out.empty()) out += ", ";
        out += sql::identifier(column.name);
    }
    return out;
}

namespace {

Result<void> createIfMissing(duckdb::Connection& conn, const std::string& qualified_name,
                             std::span<const schema::ColumnDef> columns, std::string_view partition_clause) {
    std::string ddl = std::format("CREATE TABLE IF NOT EXISTS {} (", qualified_name);
    for (std::size_t i = 0; i < columns.size(); ++i) {
        ddl += std::format("{}{} {}", i ? ", " : "", sql::identifier(columns[i].name), columns[i].type);
    }
    ddl += ")";
    if (!partition_clause.empty()) ddl += std::format(" {}", partition_clause);
    return run(conn, ddl);
}

}  // namespace

Result<void> ensureLogsTable(duckdb::Connection& conn, const std::string& qualified_name,
                             const schema::TableSchema& schema, std::string_view partition_clause) {
    if (auto created = createIfMissing(conn, qualified_name, schema.logs(), partition_clause); !created) {
        return created;
    }
    auto described = query(conn, std::format("DESCRIBE {}", qualified_name));
    if (!described) return std::unexpected(described.error());
    auto& result = **described;

    const auto base = schema::kLogsColumns.size();
    std::vector<std::string> extra;
    std::string mismatch;
    if (result.RowCount() < base) {
        mismatch = std::format("has {} columns, expected at least {}", result.RowCount(), base);
    }
    for (std::size_t i = 0; mismatch.empty() && i < result.RowCount(); ++i) {
        const auto name = result.GetValue(0, i).ToString();
        const auto type = result.GetValue(1, i).ToString();
        if (i < base) {
            const auto& expected = schema::kLogsColumns[i];
            if (name != expected.name || type != expected.type) {
                mismatch = std::format("column {} is {} {}, expected {} {}", i, name, type, expected.name,
                                       expected.type);
            }
        } else if (type != "VARCHAR") {
            mismatch = std::format("column {} ({}) is {}; columns after the base ones must be VARCHAR "
                                   "promoted attributes",
                                   i, name, type);
        } else {
            extra.push_back(name);
        }
    }
    if (!mismatch.empty()) return fatal(std::format("table {} {}", qualified_name, mismatch));

    for (const auto& column : std::span(schema.logs()).subspan(base)) {
        if (std::ranges::find(extra, column.name) != extra.end()) continue;
        if (auto added = run(conn, std::format("ALTER TABLE {} ADD COLUMN {} VARCHAR", qualified_name,
                                               sql::identifier(column.name)));
            !added) {
            return fatal(std::format("cannot add promoted column {} to {}: {}", column.name, qualified_name,
                                     added.error().message));
        }
        std::cout << std::format("Added promoted column {} to {}\n", column.name, qualified_name);
    }
    return {};
}

Result<void> ensureTable(duckdb::Connection& conn, const std::string& qualified_name,
                         std::span<const schema::ColumnDef> columns,
                         std::string_view partition_clause) {
    if (auto created = createIfMissing(conn, qualified_name, columns, partition_clause); !created) {
        return created;
    }

    auto described = query(conn, std::format("DESCRIBE {}", qualified_name));
    if (!described) return std::unexpected(described.error());
    auto& result = **described;

    // The appender owns these tables; a different shape means another
    // version or tool wrote them, and appending could corrupt the data
    std::string mismatch;
    if (result.RowCount() != columns.size()) {
        mismatch = std::format("has {} columns, expected {}", result.RowCount(), columns.size());
    }
    for (std::size_t i = 0; mismatch.empty() && i < columns.size(); ++i) {
        const auto name = result.GetValue(0, i).ToString();
        const auto type = result.GetValue(1, i).ToString();
        if (name != columns[i].name || type != columns[i].type) {
            mismatch = std::format("column {} is {} {}, expected {} {}", i, name, type,
                                   columns[i].name, columns[i].type);
        }
    }
    if (!mismatch.empty()) {
        return fatal(std::format("table {} {}", qualified_name, mismatch));
    }
    return {};
}

DuckdbSession::DuckdbSession(TableNames tables, std::shared_ptr<const schema::TableSchema> schema)
    : db_(std::make_unique<duckdb::DuckDB>(nullptr)), tables_(std::move(tables)), schema_(std::move(schema)) {}

// Iceberg defines day()/hour() of a timestamptz in UTC, but DuckDB applies
// its session time zone (by default the machine's) when it computes partition
// values. Off UTC, rows land in the neighbouring day's partition and readers
// prune them away. Without ICU DuckDB has no time zones and already uses UTC.
Result<void> DuckdbSession::pinUtc() {
    auto conn = connect();
    auto result = conn->Query("SET GLOBAL TimeZone = 'UTC'");
    if (result->HasError() && result->GetError().find("TimeZone") == std::string::npos) {
        return fatal(std::format("cannot set the DuckDB time zone to UTC: {}", result->GetError()));
    }
    return {};
}

Result<std::unique_ptr<DuckdbSession>> DuckdbSession::openLocal(
    const TableNames& tables, std::shared_ptr<const schema::TableSchema> schema) {
    try {
        std::unique_ptr<DuckdbSession> session(new DuckdbSession(tables, std::move(schema)));
        if (auto r = session->pinUtc(); !r) return std::unexpected(r.error());
        auto conn = session->connect();
        if (auto r = ensureLogsTable(*conn, tables.logs, *session->schema()); !r) {
            return std::unexpected(r.error());
        }
        if (auto r = ensureTable(*conn, tables.offsets, schema::defs(schema::kOffsetsColumns)); !r) {
            return std::unexpected(r.error());
        }
        return session;
    } catch (const std::exception& e) {
        return fatal(std::format("cannot open DuckDB: {}", e.what()));
    }
}

Result<std::unique_ptr<DuckdbSession>> DuckdbSession::openIceberg(const AppenderConfig& config) {
    std::unique_ptr<DuckdbSession> session;
    try {
        session.reset(new DuckdbSession({config.qualifiedLogsTable(), config.qualifiedOffsetsTable()},
                                        std::make_shared<schema::TableSchema>(config.promoted_attributes)));
    } catch (const std::exception& e) {
        return fatal(std::format("cannot open DuckDB: {}", e.what()));
    }
    if (auto r = session->pinUtc(); !r) return std::unexpected(r.error());
    auto conn = session->connect();

    // Extension installs need a writable home; container HOMEs often are not
    const std::string setup[] = {
        std::format("SET home_directory = {}", sql::literal(config.duckdb_home_directory)),
        "INSTALL httpfs",
        "LOAD httpfs",
        "INSTALL iceberg",
        "LOAD iceberg",
        std::format("CREATE OR REPLACE SECRET appender_s3 (TYPE S3, KEY_ID {}, SECRET {}, "
                    "REGION {}, ENDPOINT {}, USE_SSL {}, URL_STYLE {})",
                    sql::literal(config.s3_access_key), sql::literal(config.s3_secret_key),
                    sql::literal(config.s3_region), sql::literal(config.s3_endpoint),
                    config.s3_use_ssl ? "true" : "false", sql::literal(config.s3_url_style)),
        std::format("ATTACH {} AS {} (TYPE ICEBERG, ENDPOINT {}, AUTHORIZATION_TYPE 'none', "
                    "ACCESS_DELEGATION_MODE {})",
                    sql::literal(config.warehouse), AppenderConfig::kCatalogAlias,
                    sql::literal(config.catalog_uri), sql::literal(config.access_delegation_mode)),
        std::format("CREATE SCHEMA IF NOT EXISTS {}.{}", AppenderConfig::kCatalogAlias,
                    sql::identifier(config.iceberg_namespace)),
    };
    for (const auto& statement : setup) {
        if (auto r = run(*conn, statement); !r) {
            // Do not echo statements: the secret carries credentials
            return fatal(std::format("DuckDB/Iceberg setup failed: {}", r.error().message));
        }
    }

    const auto& tables = session->tables();
    if (auto r = ensureLogsTable(*conn, tables.logs, *session->schema(), partitionClause(config.partition_spec));
        !r) {
        return std::unexpected(r.error());
    }
    if (auto r = ensureTable(*conn, tables.offsets, schema::defs(schema::kOffsetsColumns)); !r) {
        return std::unexpected(r.error());
    }
    std::cout << std::format("Iceberg tables ready: {} and {}\n", tables.logs, tables.offsets);
    return session;
}

}  // namespace appender
