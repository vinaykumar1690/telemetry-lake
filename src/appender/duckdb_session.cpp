#include "duckdb_session.hpp"

#include "sql.hpp"

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

std::string columnList(std::span<const schema::Column> columns) {
    std::string out;
    for (const auto& column : columns) {
        if (!out.empty()) out += ", ";
        out += sql::identifier(column.name);
    }
    return out;
}

Result<void> ensureTable(duckdb::Connection& conn, const std::string& qualified_name,
                         std::span<const schema::Column> columns,
                         std::string_view partition_clause) {
    std::string ddl = std::format("CREATE TABLE IF NOT EXISTS {} (", qualified_name);
    for (std::size_t i = 0; i < columns.size(); ++i) {
        ddl += std::format("{}{} {}", i ? ", " : "", sql::identifier(columns[i].name),
                           columns[i].type);
    }
    ddl += ")";
    if (!partition_clause.empty()) ddl += std::format(" {}", partition_clause);
    if (auto created = run(conn, ddl); !created) return created;

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

DuckdbSession::DuckdbSession(TableNames tables)
    : db_(std::make_unique<duckdb::DuckDB>(nullptr)), tables_(std::move(tables)) {}

Result<std::unique_ptr<DuckdbSession>> DuckdbSession::openLocal(const TableNames& tables) {
    try {
        std::unique_ptr<DuckdbSession> session(new DuckdbSession(tables));
        auto conn = session->connect();
        if (auto r = ensureTable(*conn, tables.logs, schema::kLogsColumns); !r) {
            return std::unexpected(r.error());
        }
        if (auto r = ensureTable(*conn, tables.offsets, schema::kOffsetsColumns); !r) {
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
        session.reset(new DuckdbSession({config.qualifiedLogsTable(), config.qualifiedOffsetsTable()}));
    } catch (const std::exception& e) {
        return fatal(std::format("cannot open DuckDB: {}", e.what()));
    }
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
    if (auto r = ensureTable(*conn, tables.logs, schema::kLogsColumns,
                             partitionClause(config.partition_spec));
        !r) {
        return std::unexpected(r.error());
    }
    if (auto r = ensureTable(*conn, tables.offsets, schema::kOffsetsColumns); !r) {
        return std::unexpected(r.error());
    }
    std::cout << std::format("Iceberg tables ready: {} and {}\n", tables.logs, tables.offsets);
    return session;
}

}  // namespace appender
