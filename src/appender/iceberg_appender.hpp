#ifndef ICEBERG_APPENDER_HPP
#define ICEBERG_APPENDER_HPP

#include "../config.hpp"
#include "log_transformer.hpp"
#include "duckdb.hpp"
#include <vector>
#include <memory>
#include <string>

using duckdb::DuckDB;
using duckdb::Connection;

class IcebergAppender {
public:
    IcebergAppender(const AppenderConfig& config);
    ~IcebergAppender();

    // Initialize DuckDB connection and create Iceberg table if needed
    bool initialize();

    // Insert transformed log records into the buffer
    // Returns true if flush was triggered (buffer threshold met)
    bool append(const std::vector<TransformedLogRecord>& records);

    // Force flush of buffered data to Iceberg
    bool flush();

    // Get approximate buffer size in bytes
    size_t getBufferSize() const { return buffer_size_bytes_; }

    // Get number of records in buffer
    size_t getBufferRecordCount() const { return buffer_records_; }

private:
    AppenderConfig config_;
    std::unique_ptr<DuckDB> db_;
    std::unique_ptr<Connection> conn_;
    
    size_t buffer_size_bytes_;
    size_t buffer_records_;
    
    // Create Iceberg table if it doesn't exist
    bool createTableIfNotExists();
    
    // Configure S3 and Iceberg catalog
    bool configureStorage();
    
    // Insert records using DuckDB
    bool insertRecords(const std::vector<TransformedLogRecord>& records);
};

#endif // ICEBERG_APPENDER_HPP

