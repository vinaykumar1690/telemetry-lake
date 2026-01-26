#ifndef ICEBERG_APPENDER_HPP
#define ICEBERG_APPENDER_HPP

#include "../config.hpp"
#include "log_transformer.hpp"
#include "duckdb.hpp"
#include <vector>
#include <memory>
#include <string>
#include <mutex>

using duckdb::DuckDB;
using duckdb::Connection;

class IcebergAppender {
public:
    IcebergAppender(const AppenderConfig& config);
    ~IcebergAppender();

    // Initialize DuckDB connection and create Iceberg table if needed
    bool initialize();

    // Insert transformed log records into the DuckDB buffer table
    // Returns true if flush was triggered (buffer threshold met)
    bool append(const std::vector<TransformedLogRecord>& records);

    // Force flush of buffered data from DuckDB temp table to Iceberg
    bool flush();

    // Get approximate buffer size in bytes
    size_t getBufferSize() const { return buffer_size_bytes_; }

    // Get number of records in buffer
    size_t getBufferRecordCount() const { return buffer_records_; }

private:
    AppenderConfig config_;
    std::unique_ptr<DuckDB> db_;
    std::unique_ptr<Connection> conn_;
    std::mutex db_mutex_;

    size_t buffer_size_bytes_;
    size_t buffer_records_;

    // Fully qualified table name (catalog.schema.table)
    std::string full_table_name_;

    // Load required DuckDB extensions
    bool loadExtensions();

    // Configure S3 and attach Iceberg catalog
    bool configureStorage();

    // Create staging (temp) table for buffering
    bool createStagingTable();

    // Create Iceberg table if it doesn't exist
    bool createTableIfNotExists();

    // Insert records into DuckDB staging table
    bool insertToStagingTable(const std::vector<TransformedLogRecord>& records);

    // Helper to escape SQL strings
    static std::string escapeSqlString(const std::string& str);

    // Helper to format timestamp for SQL
    static std::string formatTimestamp(const std::chrono::system_clock::time_point& tp);

    // Helper to format attributes map for SQL
    static std::string formatAttributesMap(const std::map<std::string, std::string>& attrs);
};

#endif // ICEBERG_APPENDER_HPP

