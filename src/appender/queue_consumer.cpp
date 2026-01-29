#include "queue_consumer.hpp"
#include "telemetry_wrapper.pb.h"
#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"
#include <google/protobuf/util/json_util.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <cppkafka/cppkafka.h>

QueueConsumer::QueueConsumer(const AppenderConfig& config)
    : config_(config), running_(false) {
}

QueueConsumer::~QueueConsumer() {
    stop();
}

bool QueueConsumer::initialize() {
    try {
        // Create Kafka configuration
        kafka_config_ = std::make_unique<cppkafka::Configuration>(cppkafka::Configuration{
            {"metadata.broker.list", config_.queue_brokers},
            {"group.id", config_.consumer_group},
            {"enable.auto.commit", "false"},  // Manual commit for at-least-once semantics
            {"auto.offset.reset", "earliest"},
            {"enable.partition.eof", "false"},
        });

        // Create consumer
        consumer_ = std::make_unique<cppkafka::Consumer>(*kafka_config_);

        // Set rebalance callbacks on the consumer
        consumer_->set_assignment_callback([this](cppkafka::TopicPartitionList& partitions) {
            std::vector<int32_t> partition_ids;
            for (const auto& tp : partitions) {
                partition_ids.push_back(tp.get_partition());
            }
            if (assignment_callback_) {
                assignment_callback_(partition_ids);
            }
        });

        consumer_->set_revocation_callback([this](const cppkafka::TopicPartitionList& partitions) {
            std::vector<int32_t> partition_ids;
            for (const auto& tp : partitions) {
                partition_ids.push_back(tp.get_partition());
            }
            if (revocation_callback_) {
                revocation_callback_(partition_ids);
            }
        });

        // Subscribe to topic
        consumer_->subscribe({config_.queue_topic});

        std::cout << "QueueConsumer initialized with brokers: " << config_.queue_brokers
                  << ", topic: " << config_.queue_topic
                  << ", group: " << config_.consumer_group << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to initialize QueueConsumer: " << e.what() << std::endl;
        return false;
    }
}

void QueueConsumer::start(MessageCallback callback) {
    if (running_) {
        std::cerr << "Consumer is already running" << std::endl;
        return;
    }

    if (!consumer_) {
        std::cerr << "Consumer not initialized. Call initialize() first." << std::endl;
        return;
    }

    running_ = true;
    std::cout << "Starting queue consumer..." << std::endl;

    try {
        while (running_) {
            // Poll for messages with timeout
            cppkafka::Message msg = consumer_->poll(std::chrono::milliseconds(1000));

            if (msg) {
                if (msg.get_error()) {
                    if (msg.is_eof()) {
                        // End of partition, continue
                        continue;
                    } else {
                        std::cerr << "Consumer error: " << msg.get_error() << std::endl;
                        continue;
                    }
                }

                // Deserialize wrapper and parse payload
                try {
                    auto wrapper = deserializeWrapper(msg.get_payload());
                    auto request = parsePayload(wrapper);

                    // Create Kafka metadata for callback
                    KafkaMessageMeta meta;
                    meta.topic = msg.get_topic();
                    meta.partition = msg.get_partition();
                    meta.offset = msg.get_offset();

                    callback(request, meta);

                    // Note: Offset is NOT committed here anymore.
                    // The caller must track offsets and commit after successful Iceberg flush.
                } catch (const std::exception& e) {
                    std::cerr << "Error processing message: " << e.what() << std::endl;
                    // Don't track offset on error - will retry on restart
                }
            }
        }
    } catch (const cppkafka::HandleException& e) {
        std::cerr << "Kafka error in consumer: " << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Unexpected error in consumer: " << e.what() << std::endl;
    }

    running_ = false;
    std::cout << "Queue consumer stopped" << std::endl;
}

void QueueConsumer::stop() {
    if (!running_) {
        return;
    }

    running_ = false;

    if (consumer_) {
        try {
            consumer_->unsubscribe();
        } catch (const std::exception& e) {
            std::cerr << "Error during consumer shutdown: " << e.what() << std::endl;
        }
    }
}

telemetry::v1::RawTelemetryMessage QueueConsumer::deserializeWrapper(const std::string& data) {
    telemetry::v1::RawTelemetryMessage wrapper;
    if (!wrapper.ParseFromString(data)) {
        throw std::runtime_error("Failed to deserialize RawTelemetryMessage wrapper");
    }
    return wrapper;
}

opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest QueueConsumer::parsePayload(
    const telemetry::v1::RawTelemetryMessage& wrapper) {

    opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest request;
    const std::string& content_type = wrapper.content_type();
    const std::string& payload = wrapper.payload();

    if (content_type == "application/x-protobuf" || content_type == "application/protobuf") {
        if (!request.ParseFromString(payload)) {
            throw std::runtime_error("Failed to parse Protobuf payload");
        }
    } else if (content_type == "application/json" || content_type == "text/json") {
        auto status = google::protobuf::util::JsonStringToMessage(payload, &request);
        if (!status.ok()) {
            throw std::runtime_error("Failed to parse JSON payload: " + status.ToString());
        }
    } else {
        throw std::runtime_error("Unsupported content type: " + content_type);
    }

    return request;
}

void QueueConsumer::trackOffset(int32_t partition, int64_t offset) {
    // Track the max offset seen for each partition
    auto it = pending_offsets_.find(partition);
    if (it == pending_offsets_.end() || offset > it->second) {
        pending_offsets_[partition] = offset;
    }
}

std::map<int32_t, int64_t> QueueConsumer::getPendingOffsets() const {
    return pending_offsets_;
}

bool QueueConsumer::commitPendingOffsets() {
    if (!consumer_ || pending_offsets_.empty()) {
        return true; // Nothing to commit
    }

    try {
        // Build list of topic-partition-offsets to commit
        std::vector<cppkafka::TopicPartition> offsets_to_commit;
        for (const auto& kv : pending_offsets_) {
            // Commit offset + 1, as Kafka commits the NEXT offset to read
            offsets_to_commit.emplace_back(config_.queue_topic, kv.first, kv.second + 1);
        }

        consumer_->commit(offsets_to_commit);
        std::cout << "Committed offsets for " << offsets_to_commit.size() << " partitions" << std::endl;
        return true;
    } catch (const cppkafka::HandleException& e) {
        std::cerr << "Error committing offsets: " << e.what() << std::endl;
        return false;
    } catch (const std::exception& e) {
        std::cerr << "Unexpected error committing offsets: " << e.what() << std::endl;
        return false;
    }
}

void QueueConsumer::clearPendingOffsets() {
    pending_offsets_.clear();
}

bool QueueConsumer::seekToOffsets(const std::map<int32_t, int64_t>& offsets) {
    if (!consumer_ || offsets.empty()) {
        return true; // Nothing to seek
    }

    try {
        // Get current assignment to know which partitions we have
        auto assignment = consumer_->get_assignment();

        for (const auto& kv : offsets) {
            int32_t partition = kv.first;
            // Seek to offset + 1 (the next message after the one already written to Iceberg)
            int64_t seek_offset = kv.second + 1;

            // Create TopicPartition for seeking
            cppkafka::TopicPartition tp(config_.queue_topic, partition, seek_offset);

            // Check if we're assigned this partition
            bool is_assigned = false;
            for (const auto& assigned_tp : assignment) {
                if (assigned_tp.get_partition() == partition) {
                    is_assigned = true;
                    break;
                }
            }

            if (is_assigned) {
                consumer_->assign({tp});
                std::cout << "Sought partition " << partition << " to offset " << seek_offset
                          << " (max committed: " << kv.second << ")" << std::endl;
            } else {
                std::cout << "Partition " << partition << " not assigned to this consumer, skipping seek" << std::endl;
            }
        }

        // Re-assign all original partitions after seeking
        consumer_->assign(assignment);

        return true;
    } catch (const cppkafka::HandleException& e) {
        std::cerr << "Error seeking to offsets: " << e.what() << std::endl;
        return false;
    } catch (const std::exception& e) {
        std::cerr << "Unexpected error seeking to offsets: " << e.what() << std::endl;
        return false;
    }
}

bool QueueConsumer::seekPartition(int32_t partition, int64_t offset) {
    if (!consumer_) {
        return false;
    }

    try {
        // Get current assignment
        auto assignment = consumer_->get_assignment();

        // Build new assignment list with updated offset for this partition
        std::vector<cppkafka::TopicPartition> new_assignment;
        for (const auto& tp : assignment) {
            if (tp.get_partition() == partition) {
                new_assignment.emplace_back(config_.queue_topic, partition, offset);
            } else {
                new_assignment.push_back(tp);
            }
        }

        consumer_->assign(new_assignment);
        std::cout << "Sought partition " << partition << " to offset " << offset << std::endl;
        return true;
    } catch (const cppkafka::HandleException& e) {
        std::cerr << "Error seeking partition " << partition << ": " << e.what() << std::endl;
        return false;
    } catch (const std::exception& e) {
        std::cerr << "Unexpected error seeking partition " << partition << ": " << e.what() << std::endl;
        return false;
    }
}

bool QueueConsumer::commitPartitionOffset(int32_t partition, int64_t offset) {
    if (!consumer_) {
        return false;
    }

    try {
        // Commit offset + 1, as Kafka commits the NEXT offset to read
        std::vector<cppkafka::TopicPartition> offsets_to_commit;
        offsets_to_commit.emplace_back(config_.queue_topic, partition, offset + 1);

        consumer_->commit(offsets_to_commit);
        std::cout << "Committed offset " << offset << " for partition " << partition << std::endl;
        return true;
    } catch (const cppkafka::HandleException& e) {
        std::cerr << "Error committing offset for partition " << partition << ": " << e.what() << std::endl;
        return false;
    } catch (const std::exception& e) {
        std::cerr << "Unexpected error committing offset for partition " << partition << ": " << e.what() << std::endl;
        return false;
    }
}

void QueueConsumer::setAssignmentCallback(PartitionAssignmentCallback callback) {
    assignment_callback_ = std::move(callback);
}

void QueueConsumer::setRevocationCallback(PartitionRevocationCallback callback) {
    revocation_callback_ = std::move(callback);
}

