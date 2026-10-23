#include "duckdb_iceberg.hpp"

#include "schema.hpp"
#include "sql.hpp"

#include "observability/log.hpp"

#include <nlohmann/json.hpp>

#include <charconv>
#include <format>

namespace appender {
namespace {

std::string timestampLiteral(std::chrono::system_clock::time_point tp) {
    return std::format("TIMESTAMPTZ '{:%F %T}+00'",
                       std::chrono::floor<std::chrono::microseconds>(tp));
}

std::string insertLogsSql(std::span<const SealedBuffer> buffers, const TableNames& tables,
                          const schema::TableSchema& schema, const SinkIdentity& identity,
                          const std::string& ingested_at) {
    std::string selects;
    for (const auto& buffer : buffers) {
        if (buffer.rows == 0) continue;
        if (!selects.empty()) selects += "\nUNION ALL ";
        // The topic and _ingested_at are constants per commit; every other
        // column comes from the staging table, in the logs table's order
        std::string values;
        for (const auto& column : schema.logs()) {
            if (!values.empty()) values += ", ";
            if (column.name == "_kafka_topic") values += sql::literal(identity.topic);
            else if (column.name == "_ingested_at") values += ingested_at;
            else values += sql::identifier(column.name);
        }
        selects += std::format("SELECT {} FROM {}", values, sql::identifier(buffer.table));
    }
    if (selects.empty()) return {};
    return std::format("INSERT INTO {} ({})\n{}", tables.logs, columnList(schema.logs()), selects);
}

}  // namespace

std::string buildCommitSql(std::span<const SealedBuffer> buffers, const TableNames& tables,
                           const schema::TableSchema& schema, const SinkIdentity& identity,
                           const CommitContext& context) {
    const auto now = timestampLiteral(context.ingested_at);
    std::string statements = insertLogsSql(buffers, tables, schema, identity, now);
    if (!statements.empty()) statements += ";\n";

    std::string values;
    for (const auto& buffer : buffers) {
        if (!values.empty()) values += ", ";
        values += std::format("({}, {}, {}, {}, {}, {})", sql::literal(identity.consumer_group),
                              sql::literal(identity.topic), buffer.partition, buffer.max_offset,
                              now, sql::literal(identity.appender_id));
    }
    statements += std::format("INSERT INTO {} ({}) VALUES {};", tables.offsets,
                              columnList(schema::defs(schema::kOffsetsColumns)), values);
    return statements;
}

DuckdbIcebergSink::DuckdbIcebergSink(DuckdbSession& session, TableNames tables,
                                     SinkIdentity identity)
    : conn_(session.connect()),
      tables_(std::move(tables)),
      schema_(session.schema()),
      identity_(std::move(identity)) {}

Result<void> DuckdbIcebergSink::commit(std::span<const SealedBuffer> buffers,
                                       const CommitContext& context) {
    if (buffers.empty()) return {};

    if (auto begun = query(*conn_, "BEGIN TRANSACTION"); !begun) {
        return std::unexpected(begun.error());
    }
    // Everything before COMMIT only writes data files; nothing is visible yet
    if (auto inserted = query(*conn_, buildCommitSql(buffers, tables_, *schema_, identity_, context));
        !inserted) {
        (void)query(*conn_, "ROLLBACK");
        return std::unexpected(inserted.error());
    }
    if (auto committed = query(*conn_, "COMMIT"); !committed) {
        (void)query(*conn_, "ROLLBACK");
        // The catalog may have applied the commit even though we saw an error
        // (timeout, 5xx); the caller must check the offsets table
        if (committed.error().kind == ErrorKind::Fatal) return std::unexpected(committed.error());
        return indeterminate(committed.error().message);
    }
    return {};
}

std::optional<CommitFiles> commitFilesFromMetadata(std::string_view metadata_json,
                                                   std::size_t rows) {
    const auto metadata = nlohmann::json::parse(metadata_json, nullptr, /*allow_exceptions=*/false);
    if (!metadata.is_object()) return std::nullopt;
    const auto current = metadata.find("current-snapshot-id");
    const auto snapshots = metadata.find("snapshots");
    if (current == metadata.end() || snapshots == metadata.end() || !snapshots->is_array()) {
        return std::nullopt;
    }
    // Summary values are strings in Iceberg table metadata
    auto count = [](const nlohmann::json& summary, const char* key) -> std::optional<std::uint64_t> {
        auto it = summary.find(key);
        if (it == summary.end() || !it->is_string()) return std::nullopt;
        const auto& text = it->get_ref<const std::string&>();
        std::uint64_t value = 0;
        auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
        if (ec != std::errc{} || ptr != text.data() + text.size()) return std::nullopt;
        return value;
    };
    for (const auto& snapshot : *snapshots) {
        if (snapshot.value("snapshot-id", nlohmann::json()) != *current) continue;
        const auto summary = snapshot.value("summary", nlohmann::json::object());
        // Another writer (compaction) may have committed since ours
        if (summary.value("operation", "") != "append" || count(summary, "added-records") != rows) {
            return std::nullopt;
        }
        auto added = count(summary, "added-data-files");
        if (!added) return std::nullopt;
        return CommitFiles{*added, count(summary, "total-data-files")};
    }
    return std::nullopt;
}

std::optional<CommitFiles> DuckdbIcebergSink::lastCommitFiles(std::size_t rows) {
    auto result = query(*conn_, std::format(
        // ::JSON, not ::VARCHAR: a VARIANT cast to text uses DuckDB's struct notation
        "SELECT metadata::JSON FROM iceberg_load_table_response({})", tables_.logs));
    if (!result || (*result)->RowCount() == 0) {
        obs::debug("sink", "reading the snapshot summary failed",
                   {{"error", result ? std::string("no rows") : result.error().message}});
        return std::nullopt;
    }
    return commitFilesFromMetadata((*result)->GetValue(0, 0).ToString(), rows);
}

DuckdbOffsetStore::DuckdbOffsetStore(DuckdbSession& session, std::string offsets_table,
                                     std::string topic)
    : conn_(session.connect()), offsets_table_(std::move(offsets_table)), topic_(std::move(topic)) {}

Result<std::map<std::int32_t, std::int64_t>> DuckdbOffsetStore::lastCommitted(
    std::span<const std::int32_t> partitions) {
    std::map<std::int32_t, std::int64_t> out;
    if (partitions.empty()) return out;

    std::string in_list;
    for (auto partition : partitions) {
        in_list += std::format("{}{}", in_list.empty() ? "" : ", ", partition);
    }
    auto result = query(*conn_, std::format(
        "SELECT \"partition\", max(last_offset) FROM {} WHERE topic = {} AND \"partition\" IN ({}) "
        "GROUP BY \"partition\"",
        offsets_table_, sql::literal(topic_), in_list));
    if (!result) return std::unexpected(result.error());
    auto& rows = **result;
    for (duckdb::idx_t i = 0; i < rows.RowCount(); ++i) {
        out[rows.GetValue(0, i).GetValue<std::int32_t>()] = rows.GetValue(1, i).GetValue<std::int64_t>();
    }
    return out;
}

Result<void> DuckdbOffsetStore::prune() {
    // Deleting rows without rewriting files only adds delete files, which makes
    // recovery reads slower; prune only where DuckDB can compact the table
    if (!rewrite_available_) {
        auto probe = query(*conn_,
                           "SELECT count(*) FROM duckdb_functions() "
                           "WHERE function_name = 'iceberg_rewrite_data_files'");
        if (!probe) return std::unexpected(probe.error());
        rewrite_available_ = (*probe)->GetValue(0, 0).GetValue<std::int64_t>() > 0;
        if (!*rewrite_available_) {
            obs::info("sink", "offsets table maintenance skipped: this DuckDB build has no "
                              "iceberg_rewrite_data_files; compact the table externally",
                      {{"table", offsets_table_}});
        }
    }
    if (!*rewrite_available_) return {};

    // Keep the newest row per partition, plus anything from the last hour
    auto deleted = query(*conn_, std::format(
        "DELETE FROM {0} AS o WHERE o.topic = {1} AND o.committed_at < now() - INTERVAL 1 HOUR "
        "AND o.last_offset < (SELECT max(i.last_offset) FROM {0} AS i "
        "WHERE i.topic = o.topic AND i.\"partition\" = o.\"partition\")",
        offsets_table_, sql::literal(topic_)));
    if (!deleted) return std::unexpected(deleted.error());
    auto rewritten = query(*conn_, std::format("CALL iceberg_rewrite_data_files({})", offsets_table_));
    if (!rewritten) return std::unexpected(rewritten.error());
    return {};
}

}  // namespace appender
