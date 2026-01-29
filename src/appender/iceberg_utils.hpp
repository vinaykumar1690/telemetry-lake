#ifndef ICEBERG_UTILS_HPP
#define ICEBERG_UTILS_HPP

#include "../config.hpp"
#include "log_transformer.hpp"
#include "duckdb.hpp"
#include <string>
#include <map>
#include <chrono>
#include <memory>

using duckdb::DuckDB;
using duckdb::Connection;

// Utility functions for Iceberg/DuckDB operations
// Extracted to enable per-partition workers to share common functionality
class IcebergUtils {
public:
    // SQL string escaping
    static std::string escapeSqlString(const std::string& str);

    // Format timestamp for SQL
    static std::string formatTimestamp(const std::chrono::system_clock::time_point& tp);

    // Format attributes map for SQL
    static std::string formatAttributesMap(const std::map<std::string, std::string>& attrs);

    // Load required DuckDB extensions on a connection
    static bool loadExtensions(Connection& conn);

    // Configure S3 and attach Iceberg catalog on a connection
    static bool configureStorage(Connection& conn, const AppenderConfig& config);

    // Create local buffer table with given suffix (for partition-specific tables)
    static bool createBufferTable(Connection& conn, const std::string& table_suffix = "");

    // Get fully qualified Iceberg table name
    static std::string getFullTableName(const std::string& iceberg_table_name);

    // Create Iceberg table if it doesn't exist
    static bool createIcebergTableIfNotExists(Connection& conn, const std::string& full_table_name);

    // Build INSERT statement for records into a buffer table
    static std::string buildInsertSQL(const std::vector<TransformedLogRecord>& records,
                                       const std::string& buffer_table_name);

    // Estimate size of records in bytes
    static size_t estimateRecordsSize(const std::vector<TransformedLogRecord>& records);
};

#endif // ICEBERG_UTILS_HPP
