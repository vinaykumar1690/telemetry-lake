#include "iceberg_utils.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <ctime>

std::string IcebergUtils::escapeSqlString(const std::string& str) {
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

std::string IcebergUtils::formatTimestamp(const std::chrono::system_clock::time_point& tp) {
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

std::string IcebergUtils::formatAttributesMap(const std::map<std::string, std::string>& attrs) {
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

bool IcebergUtils::loadExtensions(Connection& conn) {
    try {
        // Set home directory BEFORE loading extensions (required for Iceberg/Avro auto-install)
        conn.Query("SET home_directory='/tmp';");

        // Install and load httpfs extension for S3 access
        conn.Query("INSTALL httpfs;");
        conn.Query("LOAD httpfs;");

        // Install and load iceberg extension
        conn.Query("INSTALL iceberg;");
        conn.Query("LOAD iceberg;");

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error loading extensions: " << e.what() << std::endl;
        return false;
    }
}

bool IcebergUtils::configureStorage(Connection& conn, const AppenderConfig& config) {
    try {
        // Configure S3 credentials for httpfs
        conn.Query("SET s3_endpoint='" + escapeSqlString(config.s3_endpoint) + "';");
        conn.Query("SET s3_access_key_id='" + escapeSqlString(config.s3_access_key) + "';");
        conn.Query("SET s3_secret_access_key='" + escapeSqlString(config.s3_secret_key) + "';");
        conn.Query("SET s3_region='us-east-1';");
        conn.Query("SET s3_url_style='path';");  // Required for MinIO

        // Attach Iceberg catalog using REST catalog (Nessie)
        std::ostringstream attach_sql;
        attach_sql << "ATTACH '' AS iceberg_catalog "
                   << "(TYPE ICEBERG, "
                   << "ENDPOINT '" << escapeSqlString(config.iceberg_catalog_uri) << "', "
                   << "AUTHORIZATION_TYPE 'none');";

        auto result = conn.Query(attach_sql.str());
        if (result->HasError()) {
            std::cerr << "Error attaching Iceberg catalog: " << result->GetError() << std::endl;
            return false;
        }

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error configuring storage: " << e.what() << std::endl;
        return false;
    }
}

bool IcebergUtils::createBufferTable(Connection& conn, const std::string& table_suffix) {
    try {
        std::string table_name = "local_buffer";
        if (!table_suffix.empty()) {
            table_name += "_" + table_suffix;
        }

        std::ostringstream create_sql;
        create_sql << "CREATE TABLE IF NOT EXISTS " << table_name << " (\n"
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

        auto result = conn.Query(create_sql.str());
        if (result->HasError()) {
            std::cerr << "Error creating buffer table: " << result->GetError() << std::endl;
            return false;
        }

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error creating buffer table: " << e.what() << std::endl;
        return false;
    }
}

std::string IcebergUtils::getFullTableName(const std::string& iceberg_table_name) {
    return "iceberg_catalog.default." + iceberg_table_name;
}

bool IcebergUtils::createIcebergTableIfNotExists(Connection& conn, const std::string& full_table_name) {
    try {
        // Create namespace if it doesn't exist
        auto ns_result = conn.Query("CREATE SCHEMA IF NOT EXISTS iceberg_catalog.default;");
        if (ns_result->HasError()) {
            std::cerr << "Warning: Could not create namespace: " << ns_result->GetError() << std::endl;
            // Continue anyway - namespace might already exist
        }

        // Create Iceberg table using the attached catalog
        std::ostringstream create_sql;
        create_sql << "CREATE TABLE IF NOT EXISTS " << full_table_name << " (\n"
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

        auto result = conn.Query(create_sql.str());
        if (result->HasError()) {
            std::cerr << "Error creating Iceberg table: " << result->GetError() << std::endl;
            return false;
        }

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error creating Iceberg table: " << e.what() << std::endl;
        return false;
    }
}

std::string IcebergUtils::buildInsertSQL(const std::vector<TransformedLogRecord>& records,
                                          const std::string& buffer_table_name) {
    std::ostringstream sql;
    sql << "INSERT INTO " << buffer_table_name << " VALUES ";

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

    return sql.str();
}

size_t IcebergUtils::estimateRecordsSize(const std::vector<TransformedLogRecord>& records) {
    size_t estimated_size = 0;
    for (const auto& record : records) {
        estimated_size += record.kafka_topic.size() + sizeof(record.kafka_partition) +
                          sizeof(record.kafka_offset) + record.body.size() + record.severity.size() +
                          record.service_name.size() + record.deployment_environment.size() +
                          record.host_name.size() + record.trace_id.size() + record.span_id.size();
        for (const auto& attr : record.attributes) {
            estimated_size += attr.first.size() + attr.second.size();
        }
        estimated_size += 100; // overhead
    }
    return estimated_size;
}
