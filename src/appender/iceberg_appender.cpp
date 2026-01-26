#include "iceberg_appender.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <ctime>

#include "duckdb.hpp"

IcebergAppender::IcebergAppender(const AppenderConfig& config)
    : config_(config), buffer_size_bytes_(0), buffer_records_(0) {
}

IcebergAppender::~IcebergAppender() {
    // Flush any remaining data
    flush();
}

bool IcebergAppender::initialize() {
    try {
        // Initialize DuckDB with in-memory database for maximum speed
        db_ = std::make_unique<DuckDB>(nullptr);
        conn_ = std::make_unique<Connection>(*db_);

        // Load required extensions
        if (!loadExtensions()) {
            std::cerr << "Failed to load DuckDB extensions" << std::endl;
            return false;
        }

        // Configure storage (S3 and Iceberg catalog)
        if (!configureStorage()) {
            std::cerr << "Failed to configure storage" << std::endl;
            return false;
        }

        // Create local buffer table for staging
        if (!createStagingTable()) {
            std::cerr << "Failed to create staging table" << std::endl;
            return false;
        }

        // Create Iceberg table if it doesn't exist
        if (!createTableIfNotExists()) {
            std::cerr << "Failed to create Iceberg table" << std::endl;
            return false;
        }

        std::cout << "IcebergAppender initialized successfully" << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to initialize IcebergAppender: " << e.what() << std::endl;
        return false;
    }
}

bool IcebergAppender::loadExtensions() {
    try {
        // Set home directory BEFORE loading extensions (required for Iceberg/Avro auto-install)
        conn_->Query("SET home_directory='/tmp';");

        // Install and load httpfs extension for S3 access
        conn_->Query("INSTALL httpfs;");
        conn_->Query("LOAD httpfs;");

        // Install and load iceberg extension
        conn_->Query("INSTALL iceberg;");
        conn_->Query("LOAD iceberg;");

        std::cout << "DuckDB extensions loaded: httpfs, iceberg" << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error loading extensions: " << e.what() << std::endl;
        return false;
    }
}

bool IcebergAppender::configureStorage() {
    try {
        // Configure S3 credentials for httpfs
        conn_->Query("SET s3_endpoint='" + escapeSqlString(config_.s3_endpoint) + "';");
        conn_->Query("SET s3_access_key_id='" + escapeSqlString(config_.s3_access_key) + "';");
        conn_->Query("SET s3_secret_access_key='" + escapeSqlString(config_.s3_secret_key) + "';");
        conn_->Query("SET s3_region='us-east-1';");
        conn_->Query("SET s3_url_style='path';");  // Required for MinIO

        // Attach Iceberg catalog using REST catalog (Nessie)
        // S3 credentials are already configured globally via SET commands above
        // The catalog_uri is the REST endpoint (e.g., http://nessie:19120/api/v1)
        std::ostringstream attach_sql;
        attach_sql << "ATTACH '' AS iceberg_catalog "
                   << "(TYPE ICEBERG, "
                   << "ENDPOINT '" << escapeSqlString(config_.iceberg_catalog_uri) << "', "
                   << "AUTHORIZATION_TYPE 'none');";

        auto result = conn_->Query(attach_sql.str());
        if (result->HasError()) {
            std::cerr << "Error attaching Iceberg catalog: " << result->GetError() << std::endl;
            return false;
        }

        // Set full table name
        full_table_name_ = "iceberg_catalog.default." + config_.iceberg_table_name;

        std::cout << "Storage configured: S3 endpoint=" << config_.s3_endpoint
                  << ", catalog=" << config_.iceberg_catalog_uri
                  << ", table=" << full_table_name_ << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error configuring storage: " << e.what() << std::endl;
        return false;
    }
}

bool IcebergAppender::createStagingTable() {
    try {
        // Create a local in-memory table for buffering records
        // DuckDB can handle tens of thousands of inserts per second
        // Includes Kafka metadata columns for exactly-once semantics
        std::string create_sql = R"(
            CREATE TABLE local_buffer (
                _kafka_topic VARCHAR,
                _kafka_partition INTEGER,
                _kafka_offset BIGINT,
                timestamp TIMESTAMP,
                severity VARCHAR,
                body VARCHAR,
                trace_id VARCHAR,
                span_id VARCHAR,
                service_name VARCHAR,
                deployment_environment VARCHAR,
                host_name VARCHAR,
                attributes MAP(VARCHAR, VARCHAR)
            );
        )";

        auto result = conn_->Query(create_sql);
        if (result->HasError()) {
            std::cerr << "Error creating local buffer table: " << result->GetError() << std::endl;
            return false;
        }

        std::cout << "Local buffer table created for staging" << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error creating local buffer table: " << e.what() << std::endl;
        return false;
    }
}

bool IcebergAppender::createTableIfNotExists() {
    try {
        // Create namespace if it doesn't exist
        auto ns_result = conn_->Query("CREATE SCHEMA IF NOT EXISTS iceberg_catalog.default;");
        if (ns_result->HasError()) {
            std::cerr << "Warning: Could not create namespace: " << ns_result->GetError() << std::endl;
            // Continue anyway - namespace might already exist
        }

        // Create Iceberg table using the attached catalog
        // Includes Kafka metadata columns for exactly-once semantics
        std::ostringstream create_sql;
        create_sql << "CREATE TABLE IF NOT EXISTS " << full_table_name_ << " (\n"
                   << "  _kafka_topic VARCHAR,\n"
                   << "  _kafka_partition INTEGER,\n"
                   << "  _kafka_offset BIGINT,\n"
                   << "  timestamp TIMESTAMP,\n"
                   << "  severity VARCHAR,\n"
                   << "  body VARCHAR,\n"
                   << "  trace_id VARCHAR,\n"
                   << "  span_id VARCHAR,\n"
                   << "  service_name VARCHAR,\n"
                   << "  deployment_environment VARCHAR,\n"
                   << "  host_name VARCHAR,\n"
                   << "  attributes MAP(VARCHAR, VARCHAR)\n"
                   << ");";

        auto result = conn_->Query(create_sql.str());
        if (result->HasError()) {
            std::cerr << "Error creating Iceberg table: " << result->GetError() << std::endl;
            return false;
        }

        std::cout << "Iceberg table created or verified: " << full_table_name_ << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error creating Iceberg table: " << e.what() << std::endl;
        return false;
    }
}

bool IcebergAppender::append(const std::vector<TransformedLogRecord>& records) {
    if (records.empty()) {
        return false;
    }

    std::lock_guard<std::mutex> lock(db_mutex_);

    // Insert records into DuckDB local buffer table
    if (!insertToStagingTable(records)) {
        std::cerr << "Failed to insert records to local buffer" << std::endl;
        return false;
    }

    // Track buffer size (including Kafka metadata)
    for (const auto& record : records) {
        buffer_size_bytes_ += record.kafka_topic.size() + sizeof(record.kafka_partition) +
                             sizeof(record.kafka_offset) + record.body.size() + record.severity.size() +
                             record.service_name.size() + record.deployment_environment.size() +
                             record.host_name.size() + record.trace_id.size() + record.span_id.size();
        for (const auto& attr : record.attributes) {
            buffer_size_bytes_ += attr.first.size() + attr.second.size();
        }
        buffer_size_bytes_ += 100; // overhead
    }
    buffer_records_ += records.size();

    // Check if we should flush based on size threshold
    size_t buffer_size_mb = buffer_size_bytes_ / (1024 * 1024);
    if (buffer_size_mb >= config_.buffer_size_mb) {
        std::cout << "Buffer size threshold reached (" << buffer_size_mb << " MB), triggering flush..." << std::endl;
        return true; // Signal that flush should happen
    }

    return false;
}

bool IcebergAppender::insertToStagingTable(const std::vector<TransformedLogRecord>& records) {
    try {
        // Build INSERT statement with VALUES for batch insert
        // Includes Kafka metadata columns for exactly-once semantics
        std::ostringstream sql;
        sql << "INSERT INTO local_buffer VALUES ";

        bool first = true;
        for (const auto& record : records) {
            if (!first) {
                sql << ", ";
            }
            first = false;

            sql << "("
                << "'" << escapeSqlString(record.kafka_topic) << "', "
                << record.kafka_partition << ", "
                << record.kafka_offset << ", "
                << "'" << formatTimestamp(record.timestamp) << "', "
                << "'" << escapeSqlString(record.severity) << "', "
                << "'" << escapeSqlString(record.body) << "', "
                << "'" << escapeSqlString(record.trace_id) << "', "
                << "'" << escapeSqlString(record.span_id) << "', "
                << "'" << escapeSqlString(record.service_name) << "', "
                << "'" << escapeSqlString(record.deployment_environment) << "', "
                << "'" << escapeSqlString(record.host_name) << "', "
                << formatAttributesMap(record.attributes)
                << ")";
        }
        sql << ";";

        auto result = conn_->Query(sql.str());
        if (result->HasError()) {
            std::cerr << "Error inserting to local buffer: " << result->GetError() << std::endl;
            return false;
        }

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error inserting to local buffer: " << e.what() << std::endl;
        return false;
    }
}

bool IcebergAppender::flush() {
    if (buffer_records_ == 0) {
        return true; // Nothing to flush
    }

    std::lock_guard<std::mutex> lock(db_mutex_);

    try {
        std::cout << "Flushing " << buffer_records_ << " records ("
                  << (buffer_size_bytes_ / (1024 * 1024)) << " MB) to Iceberg..." << std::endl;

        // Note: DuckDB doesn't support transactions spanning multiple attached databases.
        // We flush to Iceberg first, then clear the local buffer.
        // If the flush succeeds but clearing fails, we may have duplicates on restart,
        // but that's acceptable for at-least-once semantics.

        // Stream from local buffer to Iceberg table
        std::ostringstream insert_sql;
        insert_sql << "INSERT INTO " << full_table_name_ << " SELECT * FROM local_buffer;";

        auto result = conn_->Query(insert_sql.str());
        if (result->HasError()) {
            std::cerr << "Error flushing to Iceberg: " << result->GetError() << std::endl;
            return false;
        }

        // Clear the local buffer (separate operation)
        result = conn_->Query("DELETE FROM local_buffer;");
        if (result->HasError()) {
            std::cerr << "Warning: Error clearing local buffer (data was written to Iceberg): "
                      << result->GetError() << std::endl;
            // Don't fail - data was written successfully to Iceberg
        }

        // Reset buffer counters
        buffer_size_bytes_ = 0;
        buffer_records_ = 0;

        std::cout << "Flush completed successfully to " << full_table_name_ << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error during flush: " << e.what() << std::endl;
        return false;
    }
}

std::map<int32_t, int64_t> IcebergAppender::getMaxCommittedOffsets(const std::string& topic) {
    std::map<int32_t, int64_t> offsets;

    try {
        std::lock_guard<std::mutex> lock(db_mutex_);

        // Query max offset per partition from Iceberg table for the given topic
        std::ostringstream sql;
        sql << "SELECT _kafka_partition, MAX(_kafka_offset) as max_offset "
            << "FROM " << full_table_name_ << " "
            << "WHERE _kafka_topic = '" << escapeSqlString(topic) << "' "
            << "GROUP BY _kafka_partition";

        auto result = conn_->Query(sql.str());
        if (result->HasError()) {
            std::cerr << "Error querying max offsets: " << result->GetError() << std::endl;
            return offsets;
        }

        // Parse results into map
        size_t row_count = result->RowCount();
        for (size_t i = 0; i < row_count; ++i) {
            int32_t partition = result->GetValue(0, i).GetValue<int32_t>();
            int64_t max_offset = result->GetValue(1, i).GetValue<int64_t>();
            offsets[partition] = max_offset;

            std::cout << "Recovery: found max offset " << max_offset
                      << " for partition " << partition
                      << " in topic " << topic << std::endl;
        }

        if (offsets.empty()) {
            std::cout << "Recovery: no existing data found for topic " << topic
                      << ", will start from auto.offset.reset policy" << std::endl;
        }

        return offsets;
    } catch (const std::exception& e) {
        std::cerr << "Error querying max offsets: " << e.what() << std::endl;
        return offsets;
    }
}

std::string IcebergAppender::escapeSqlString(const std::string& str) {
    std::string result;
    result.reserve(str.size() * 1.2);
    for (char c : str) {
        if (c == '\'') {
            result += "''";  // Escape single quotes
        } else if (c == '\\') {
            result += "\\\\";  // Escape backslashes
        } else {
            result += c;
        }
    }
    return result;
}

std::string IcebergAppender::formatTimestamp(const std::chrono::system_clock::time_point& tp) {
    auto time_t_val = std::chrono::system_clock::to_time_t(tp);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        tp.time_since_epoch()) % 1000;

    std::tm tm_val;
    gmtime_r(&time_t_val, &tm_val);

    std::ostringstream oss;
    oss << std::put_time(&tm_val, "%Y-%m-%d %H:%M:%S")
        << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return oss.str();
}

std::string IcebergAppender::formatAttributesMap(const std::map<std::string, std::string>& attrs) {
    if (attrs.empty()) {
        return "MAP([], [])";
    }

    std::ostringstream keys, values;
    keys << "[";
    values << "[";

    bool first = true;
    for (const auto& kv : attrs) {
        if (!first) {
            keys << ", ";
            values << ", ";
        }
        first = false;
        keys << "'" << escapeSqlString(kv.first) << "'";
        values << "'" << escapeSqlString(kv.second) << "'";
    }

    keys << "]";
    values << "]";

    return "MAP(" + keys.str() + ", " + values.str() + ")";
}
