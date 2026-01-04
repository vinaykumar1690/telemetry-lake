#include "queue_consumer.hpp"
#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"
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

                // Deserialize and call callback
                try {
                    auto request = deserializeMessage(msg.get_payload());
                    callback(request);
                    
                    // Commit offset after successful processing
                    consumer_->commit(msg);
                } catch (const std::exception& e) {
                    std::cerr << "Error processing message: " << e.what() << std::endl;
                    // Don't commit on error - will retry
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

opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest QueueConsumer::deserializeMessage(const std::string& data) {
    opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest request;
    if (!request.ParseFromString(data)) {
        throw std::runtime_error("Failed to deserialize ExportLogsServiceRequest");
    }
    return request;
}

