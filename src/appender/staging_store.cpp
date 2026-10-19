#include "staging_store.hpp"

#include "duckdb_session.hpp"
#include "schema.hpp"
#include "sql.hpp"
#include "utf8.hpp"

#include <algorithm>
#include <format>
#include <iostream>
#include <limits>

namespace appender {
namespace {

static_assert(schema::kStagedColumns.size() == 23, "append order below must match the schema");

void appendNull(duckdb::Appender& appender) {
    appender.Append<std::nullptr_t>(nullptr);
}

// Empty text is stored as NULL; invalid UTF-8 is replaced, never rejected
void appendText(duckdb::Appender& appender, std::string_view text) {
    if (text.empty()) {
        appendNull(appender);
    } else if (utf8::isValid(text)) {
        appender.Append(text.data(), static_cast<std::uint32_t>(text.size()));
    } else {
        const auto clean = utf8::sanitize(text);
        appender.Append(clean.data(), static_cast<std::uint32_t>(clean.size()));
    }
}

std::int64_t clampToInt64(std::uint64_t value) {
    return static_cast<std::int64_t>(
        std::min<std::uint64_t>(value, std::numeric_limits<std::int64_t>::max()));
}

duckdb::Value textValue(std::string_view text) {
    return duckdb::Value(utf8::isValid(text) ? std::string(text) : utf8::sanitize(text));
}

// MAP keys must be unique; a repeated key keeps its last value
duckdb::Value mapValue(const Attributes& attributes) {
    duckdb::vector<duckdb::Value> keys;
    duckdb::vector<duckdb::Value> values;
    std::vector<std::string_view> seen;
    keys.reserve(attributes.size());
    values.reserve(attributes.size());
    for (const auto& [key, value] : attributes) {
        if (auto it = std::ranges::find(seen, key); it != seen.end()) {
            values[static_cast<std::size_t>(it - seen.begin())] = textValue(value);
            continue;
        }
        seen.push_back(key);
        keys.push_back(textValue(key));
        values.push_back(textValue(value));
    }
    return duckdb::Value::MAP(duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
                              std::move(keys), std::move(values));
}

}  // namespace

void ByteBudget::add(std::size_t bytes) {
    std::lock_guard lock(mutex_);
    used_ += bytes;
}

void ByteBudget::release(std::size_t bytes) {
    {
        std::lock_guard lock(mutex_);
        used_ -= std::min(bytes, used_);
    }
    below_cap_.notify_all();
}

std::size_t ByteBudget::used() const {
    std::lock_guard lock(mutex_);
    return used_;
}

bool ByteBudget::waitBelowCap(std::chrono::milliseconds timeout) {
    std::unique_lock lock(mutex_);
    return below_cap_.wait_for(lock, timeout, [&] { return used_ < cap_; });
}

PartitionStaging::PartitionStaging(duckdb::DuckDB& db, std::int32_t partition, ByteBudget& budget)
    : partition_(partition), budget_(budget), conn_(db) {}

PartitionStaging::~PartitionStaging() {
    std::lock_guard lock(mutex_);
    budget_.release(bytes_);
    if (!table_.empty()) {
        (void)query(conn_, std::format("DROP TABLE IF EXISTS {}", sql::identifier(table_)));
    }
}

Result<void> PartitionStaging::init() {
    std::lock_guard lock(mutex_);
    return createGeneration();
}

Result<void> PartitionStaging::createGeneration() {
    std::string table = std::format("stg_p{}_g{}", partition_, generation_);
    std::string ddl = std::format("CREATE TABLE {} (", sql::identifier(table));
    for (std::size_t i = 0; i < schema::kStagedColumns.size(); ++i) {
        ddl += std::format("{}{} {}", i ? ", " : "", sql::identifier(schema::kStagedColumns[i].name),
                           schema::kStagedColumns[i].type);
    }
    ddl += ")";
    if (auto created = query(conn_, ddl); !created) return std::unexpected(created.error());
    table_ = std::move(table);
    ++generation_;
    return {};
}

Result<void> PartitionStaging::append(std::span<const LogRow> rows, std::int64_t max_offset,
                                      std::size_t bytes) {
    std::lock_guard lock(mutex_);
    if (!rows.empty()) {
        try {
            duckdb::Appender appender(conn_, table_);
            try {
                const ResourceFields* cached_resource = nullptr;
                duckdb::Value resource_map;
                for (const LogRow& row : rows) {
                    appender.BeginRow();
                    appender.Append<std::int32_t>(row.kafka_partition);
                    appender.Append<std::int64_t>(row.kafka_offset);
                    appender.Append<std::int32_t>(row.record_index);
                    if (row.timestamp_us) {
                        appender.Append<duckdb::Value>(
                            duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t(*row.timestamp_us)));
                    } else {
                        appendNull(appender);
                    }
                    appender.Append<std::int64_t>(clampToInt64(row.time_unix_nano));
                    appender.Append<std::int64_t>(clampToInt64(row.observed_time_unix_nano));
                    appender.Append<std::int32_t>(row.severity_number);
                    appendText(appender, row.severity_text);
                    appendText(appender, row.body);
                    appendText(appender, row.trace_id);
                    appendText(appender, row.span_id);
                    appender.Append<std::int64_t>(row.flags);
                    appendText(appender, row.event_name);

                    static const ResourceFields kNoResource;
                    const ResourceFields& resource = row.resource ? *row.resource : kNoResource;
                    appendText(appender, resource.service_name);
                    appendText(appender, resource.service_namespace);
                    appendText(appender, resource.service_instance_id);
                    appendText(appender, resource.deployment_environment);
                    appendText(appender, resource.host_name);
                    appendText(appender, row.scope ? std::string_view(row.scope->name) : "");
                    appendText(appender, row.scope ? std::string_view(row.scope->version) : "");

                    // Rows of one ResourceLogs share their resource; build its MAP once
                    if (cached_resource != &resource) {
                        resource_map = mapValue(resource.attributes);
                        cached_resource = &resource;
                    }
                    appender.Append<duckdb::Value>(resource_map);
                    appender.Append<duckdb::Value>(mapValue(row.attributes));
                    appender.Append<std::int64_t>(row.dropped_attributes_count);
                    appender.EndRow();
                }
                appender.Close();
            } catch (...) {
                appender.Clear();  // keep the batch all-or-nothing
                throw;
            }
        } catch (const std::exception& e) {
            return std::unexpected(AppenderError{classifyDuckdbError(e.what()),
                                                 std::format("staging append failed: {}", e.what())});
        }
        rows_ += rows.size();
        if (!first_append_) first_append_ = std::chrono::steady_clock::now();
    }
    bytes_ += bytes;
    budget_.add(bytes);
    max_offset_ = std::max(max_offset_, max_offset);
    dirty_ = true;
    return {};
}

void PartitionStaging::advanceOffset(std::int64_t offset) {
    std::lock_guard lock(mutex_);
    max_offset_ = std::max(max_offset_, offset);
    dirty_ = true;
    if (!first_append_) first_append_ = std::chrono::steady_clock::now();
}

Result<std::optional<SealedBuffer>> PartitionStaging::seal() {
    std::lock_guard lock(mutex_);
    if (!dirty_) return std::optional<SealedBuffer>{};

    SealedBuffer sealed{partition_, table_, max_offset_, rows_, bytes_};
    if (auto created = createGeneration(); !created) {
        table_ = sealed.table;  // keep appending to the old generation
        return std::unexpected(created.error());
    }
    rows_ = 0;
    bytes_ = 0;
    dirty_ = false;
    first_append_.reset();
    return std::optional<SealedBuffer>{std::move(sealed)};
}

void PartitionStaging::discard() {
    std::lock_guard lock(mutex_);
    if (rows_ > 0) {
        (void)query(conn_, std::format("DELETE FROM {}", sql::identifier(table_)));
    }
    budget_.release(bytes_);
    rows_ = 0;
    bytes_ = 0;
    dirty_ = false;
    first_append_.reset();
}

std::size_t PartitionStaging::bytes() const {
    std::lock_guard lock(mutex_);
    return bytes_;
}

std::optional<std::chrono::steady_clock::time_point> PartitionStaging::oldestRow() const {
    std::lock_guard lock(mutex_);
    return first_append_;
}

StagingRegistry::StagingRegistry(duckdb::DuckDB& db, std::size_t max_staged_bytes)
    : db_(db), budget_(max_staged_bytes), drop_conn_(db) {}

Result<void> StagingRegistry::add(std::int32_t partition) {
    auto staging = std::make_shared<PartitionStaging>(db_, partition, budget_);
    std::lock_guard lock(mutex_);
    if (partitions_.contains(partition)) return {};
    if (auto r = staging->init(); !r) return r;
    partitions_.emplace(partition, std::move(staging));
    return {};
}

void StagingRegistry::remove(std::int32_t partition) {
    std::shared_ptr<PartitionStaging> staging;
    {
        std::lock_guard lock(mutex_);
        auto it = partitions_.find(partition);
        if (it == partitions_.end()) return;
        staging = std::move(it->second);
        partitions_.erase(it);
    }
    staging->discard();
}

std::shared_ptr<PartitionStaging> StagingRegistry::find(std::int32_t partition) const {
    std::lock_guard lock(mutex_);
    auto it = partitions_.find(partition);
    return it == partitions_.end() ? nullptr : it->second;
}

std::vector<std::int32_t> StagingRegistry::partitions() const {
    std::lock_guard lock(mutex_);
    std::vector<std::int32_t> out;
    for (const auto& [partition, staging] : partitions_) out.push_back(partition);
    return out;
}

Result<std::vector<SealedBuffer>> StagingRegistry::sealAll(const std::set<std::int32_t>& excluded) {
    std::vector<std::shared_ptr<PartitionStaging>> targets;
    {
        std::lock_guard lock(mutex_);
        for (const auto& [partition, staging] : partitions_) {
            if (!excluded.contains(partition)) targets.push_back(staging);
        }
    }
    std::vector<SealedBuffer> sealed;
    for (const auto& staging : targets) {
        auto result = staging->seal();
        if (!result) {
            // Generations sealed so far still go out with this commit
            if (sealed.empty()) return std::unexpected(result.error());
            std::cerr << std::format("Partition {}: seal failed: {}\n", staging->partition(),
                                     result.error().message);
            continue;
        }
        if (*result) sealed.push_back(std::move(**result));
    }
    return sealed;
}

void StagingRegistry::dropSealed(const SealedBuffer& sealed) {
    {
        std::lock_guard lock(drop_mutex_);
        if (auto dropped = query(drop_conn_, std::format("DROP TABLE IF EXISTS {}",
                                                         sql::identifier(sealed.table)));
            !dropped) {
            std::cerr << std::format("Failed to drop staging table {}: {}\n", sealed.table,
                                     dropped.error().message);
        }
    }
    budget_.release(sealed.bytes);
}

void StagingRegistry::discard(const std::set<std::int32_t>& partitions) {
    for (std::int32_t partition : partitions) {
        if (auto staging = find(partition)) staging->discard();
    }
}

std::size_t StagingRegistry::unsealedBytes() const {
    std::lock_guard lock(mutex_);
    std::size_t total = 0;
    for (const auto& [partition, staging] : partitions_) total += staging->bytes();
    return total;
}

std::optional<std::chrono::steady_clock::time_point> StagingRegistry::oldestUnsealedRow() const {
    std::lock_guard lock(mutex_);
    std::optional<std::chrono::steady_clock::time_point> oldest;
    for (const auto& [partition, staging] : partitions_) {
        if (auto t = staging->oldestRow(); t && (!oldest || *t < *oldest)) oldest = t;
    }
    return oldest;
}

}  // namespace appender
