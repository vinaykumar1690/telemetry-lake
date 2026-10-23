#include "kafka_source.hpp"

#include "backoff.hpp"
#include "observability/kafka_stats.hpp"
#include "observability/log.hpp"

#include <cstdlib>
#include <format>
#include <random>
#include <thread>

namespace appender {
namespace {

std::string describe(const std::set<std::int32_t>& partitions) {
    std::string out;
    for (auto p : partitions) out += std::format("{}{}", out.empty() ? "" : ",", p);
    return out;
}

std::set<std::int32_t> ids(const cppkafka::TopicPartitionList& partitions) {
    std::set<std::int32_t> out;
    for (const auto& tp : partitions) out.insert(tp.get_partition());
    return out;
}

}  // namespace

KafkaSource::KafkaSource(KafkaSourceOptions options, StagingRegistry& staging,
                         PipelinePool& pipelines, IcebergCommitter& committer,
                         OffsetStore& offsets, FatalCallback on_fatal)
    : options_(std::move(options)),
      metrics_(options_.metrics ? options_.metrics : AppenderMetrics::standalone()),
      staging_(staging),
      pipelines_(pipelines),
      committer_(committer),
      offsets_(offsets),
      on_fatal_(std::move(on_fatal)) {}

KafkaSource::~KafkaSource() {
    flow_.clear();  // parked messages must not outlive the consumer
}

Result<void> KafkaSource::init() {
    try {
        cppkafka::Configuration config = {
            {"metadata.broker.list", options_.brokers},
            {"group.id", options_.consumer_group},
            {"enable.auto.commit", false},
            {"enable.auto.offset.store", false},
            {"enable.partition.eof", false},
            // Partitions without a durable offset start from the beginning;
            // onAssigned sets that explicitly, this covers anything else
            {"auto.offset.reset", "earliest"},
            {"max.poll.interval.ms", options_.max_poll_interval_ms},
            // Eager rebalancing: a partition is not handed to its next owner
            // before our revoke callback returns, which is what the fence relies on
            {"partition.assignment.strategy", "range,roundrobin"},
            {"statistics.interval.ms", options_.stats_interval_ms},
        };
        // Stats, log and error callbacks run inside poll(), on this thread
        if (options_.stats) {
            config.set_stats_callback([stats = options_.stats](cppkafka::KafkaHandleBase&,
                                                               const std::string& json) {
                stats->update(kClient, json);
            });
        }
        config.set_log_callback([](cppkafka::KafkaHandleBase&, int level,
                                   const std::string& facility, const std::string& message) {
            obs::logKafka(kClient, level, facility, message);
        });
        config.set_error_callback([](cppkafka::KafkaHandleBase&, int error,
                                     const std::string& reason) {
            const auto code = static_cast<rd_kafka_resp_err_t>(error);
            obs::logKafkaError(kClient, error, rd_kafka_err2name(code), reason,
                               code == RD_KAFKA_RESP_ERR__FATAL);
        });
        consumer_ = std::make_unique<cppkafka::Consumer>(config);
        consumer_->set_assignment_callback(
            [this](cppkafka::TopicPartitionList& partitions) { onAssigned(partitions); });
        consumer_->set_revocation_callback(
            [this](const cppkafka::TopicPartitionList& partitions) { onRevoked(partitions); });
        consumer_->set_rebalance_error_callback([](cppkafka::Error error) {
            obs::error("kafka", "rebalance error", {{"client", kClient}, {"error", error.to_string()}});
        });
        consumer_->subscribe({options_.topic});
    } catch (const std::exception& e) {
        return fatal(std::format("cannot create Kafka consumer: {}", e.what()));
    }
    obs::info("kafka", "consumer subscribed",
              {{"topic", options_.topic}, {"group", options_.consumer_group}});
    return {};
}

void KafkaSource::run() {
    {
        std::lock_guard lock(stats_mutex_);
        stats_.running = true;
    }
    const FlowControl::Router route = [this](Work& work) { return pipelines_.tryRoute(work); };

    while (!stop_requested_) {
        std::vector<cppkafka::Message> batch;
        try {
            // Rebalance callbacks run inside this call, on this thread
            batch = consumer_->poll_batch(options_.batch_size, options_.poll_timeout);
        } catch (const std::exception& e) {
            if (on_fatal_) on_fatal_(AppenderError{ErrorKind::Fatal, std::format("Kafka poll failed: {}", e.what())});
            break;
        }
        for (auto& message : batch) {
            if (!message) continue;
            if (message.get_error()) {
                if (message.get_error().get_error() == RD_KAFKA_RESP_ERR__FATAL) {
                    if (on_fatal_) {
                        on_fatal_(AppenderError{ErrorKind::Fatal, std::format(
                            "Kafka consumer fatal error: {}", message.get_error().to_string())});
                    }
                    stop_requested_ = true;
                    break;
                }
                const auto now = std::chrono::steady_clock::now();
                if (now - last_error_log_ > std::chrono::seconds(5)) {
                    last_error_log_ = now;
                    obs::warn("kafka", "consumer error",
                              {{"client", kClient}, {"error", message.get_error().to_string()}});
                }
                continue;
            }
            auto owner = std::make_shared<cppkafka::Message>(std::move(message));
            const auto& payload = owner->get_payload();
            const auto& key = owner->get_key();
            const auto timestamp = owner->get_timestamp();
            Work work{owner->get_partition(), owner->get_offset(),
                      std::string_view(reinterpret_cast<const char*>(key.get_data()), key.get_size()),
                      std::string_view(reinterpret_cast<const char*>(payload.get_data()),
                                       payload.get_size()),
                      owner,
                      timestamp ? timestamp->get_timestamp().count() : -1};
            flow_.offer(std::move(work), route);
        }
        flow_.drain(route);
        updatePauses();
        commitHints(/*synchronous=*/false);

        std::lock_guard lock(stats_mutex_);
        stats_.last_poll = std::chrono::steady_clock::now();
        stats_.assigned = assigned_.size();
        stats_.paused = paused_.size();
        stats_.parked = flow_.parkedCount();
    }

    std::lock_guard lock(stats_mutex_);
    stats_.running = false;
}

void KafkaSource::updatePauses() {
    const auto blocked = flow_.blocked();
    std::set<std::int32_t> to_pause;
    std::set<std::int32_t> to_resume;
    for (auto p : blocked) {
        if (assigned_.contains(p) && !paused_.contains(p)) to_pause.insert(p);
    }
    for (auto p : paused_) {
        if (!blocked.contains(p) && pipelines_.hasRoom(p)) to_resume.insert(p);
    }
    try {
        if (!to_pause.empty()) {
            consumer_->pause_partitions(partitionList(to_pause));
            paused_.insert(to_pause.begin(), to_pause.end());
        }
        if (!to_resume.empty()) {
            consumer_->resume_partitions(partitionList(to_resume));
            for (auto p : to_resume) paused_.erase(p);
        }
    } catch (const std::exception& e) {
        obs::warn("kafka", "pausing/resuming partitions failed", {{"error", e.what()}});
    }
}

void KafkaSource::onCommitted(const std::map<std::int32_t, std::int64_t>& offsets) {
    std::lock_guard lock(hints_mutex_);
    for (auto [partition, offset] : offsets) {
        auto& hint = hints_[partition];
        hint = std::max(hint, offset);
        // Only partitions still assigned; a revoke erases the entry
        if (auto it = durable_.find(partition); it != durable_.end()) {
            it->second = std::max(it->second, offset);
        }
    }
}

std::map<std::int32_t, std::int64_t> KafkaSource::durableOffsets() const {
    std::lock_guard lock(hints_mutex_);
    return durable_;
}

void KafkaSource::commitHints(bool synchronous) {
    std::map<std::int32_t, std::int64_t> hints;
    {
        std::lock_guard lock(hints_mutex_);
        hints.swap(hints_);
    }
    cppkafka::TopicPartitionList list;
    for (auto [partition, offset] : hints) {
        // Only partitions we still own; Kafka stores the next offset to read
        if (assigned_.contains(partition)) list.emplace_back(options_.topic, partition, offset + 1);
    }
    if (list.empty()) return;
    try {
        if (synchronous) consumer_->commit(list);
        else consumer_->async_commit(list);
    } catch (const std::exception& e) {
        // Hints only: the offsets table already holds the truth
        obs::warn("kafka", "offset commit (hint) failed", {{"error", e.what()}});
    }
}

cppkafka::TopicPartitionList KafkaSource::partitionList(const std::set<std::int32_t>& partitions) const {
    cppkafka::TopicPartitionList list;
    for (auto p : partitions) list.emplace_back(options_.topic, p);
    return list;
}

void KafkaSource::onAssigned(cppkafka::TopicPartitionList& partitions) {
    const auto assigned = ids(partitions);
    const std::vector<std::int32_t> list(assigned.begin(), assigned.end());
    obs::info("kafka", "partitions assigned", {{"partitions", describe(assigned)}});
    metrics_->partitions_assigned.Increment(static_cast<double>(assigned.size()));

    // Bounded retries: the catalog may be briefly unavailable, but this runs
    // inside poll() and must finish well within max.poll.interval.ms
    std::mt19937 rng{std::random_device{}()};
    Result<std::map<std::int32_t, std::optional<std::int64_t>>> resume = fatal("not attempted");
    for (int attempt = 0; attempt < 5; ++attempt) {
        if (attempt > 0) {
            std::this_thread::sleep_for(backoffWithJitter(attempt, std::chrono::milliseconds(500),
                                                          std::chrono::milliseconds(8000), rng));
        }
        resume = resumeOffsets(list, offsets_.lastCommitted(list));
        if (resume) break;
        obs::warn("kafka", "reading committed offsets failed",
                  {{"attempt", attempt + 1}, {"error", resume.error().message}});
    }
    if (!resume) {
        // Starting at any other position could duplicate or skip data
        obs::error("kafka", "cannot determine where to resume; exiting");
        std::_Exit(2);
    }

    for (auto& tp : partitions) {
        const auto& next = resume->at(tp.get_partition());
        // This list is what cppkafka assigns after the callback returns, so
        // setting the offset here is the seek
        tp.set_offset(next ? *next : RD_KAFKA_OFFSET_BEGINNING);
        obs::info("kafka", "partition resuming",
                  {{"partition", tp.get_partition()},
                   {"offset", next ? std::to_string(*next) : std::string("beginning")}});
        {
            std::lock_guard lock(hints_mutex_);
            durable_[tp.get_partition()] = next ? *next - 1 : -1;
        }
        if (auto added = staging_.add(tp.get_partition()); !added) {
            obs::error("kafka", "partition staging setup failed; exiting",
                       {{"partition", tp.get_partition()}, {"error", added.error().message}});
            std::_Exit(2);
        }
    }
    committer_.include(assigned);
    assigned_.insert(assigned.begin(), assigned.end());
}

void KafkaSource::onRevoked(const cppkafka::TopicPartitionList& partitions) {
    const auto revoked = ids(partitions);
    obs::info("kafka", "partitions revoked", {{"partitions", describe(revoked)}});
    metrics_->partitions_revoked.Increment(static_cast<double>(revoked.size()));

    // Until this returns the group will not hand these partitions to anyone
    // else, so any commit for them must be finished (or abandoned) by then
    const auto fence_started = std::chrono::steady_clock::now();
    const auto deadline = fence_started + options_.revoke_timeout;
    if (!committer_.fence(revoked, deadline)) {
        obs::error("kafka",
                   "an Iceberg commit is still in flight after the revoke timeout; exiting so it "
                   "cannot race the next owner",
                   {{"partitions", describe(revoked)},
                    {"timeout_s", options_.revoke_timeout.count()}});
        std::_Exit(2);
    }
    metrics_->revoke_fence_duration.Observe(
        std::chrono::duration<double>(std::chrono::steady_clock::now() - fence_started).count());
    pipelines_.dropPartitions(revoked);
    flow_.forget(revoked);
    committer_.discard(revoked);
    for (auto p : revoked) staging_.remove(p);

    commitHints(/*synchronous=*/true);  // last hints while we still own them
    for (auto p : revoked) {
        assigned_.erase(p);
        paused_.erase(p);
    }
    std::lock_guard lock(hints_mutex_);
    for (auto p : revoked) durable_.erase(p);
}

void KafkaSource::shutdown() {
    if (!consumer_) return;
    commitHints(/*synchronous=*/true);
    flow_.clear();
    consumer_.reset();  // leaves the group
}

KafkaSourceStats KafkaSource::stats() const {
    std::lock_guard lock(stats_mutex_);
    return stats_;
}

}  // namespace appender
