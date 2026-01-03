#include "queue_producer.hpp"
#include <iostream>
#include <chrono>
#include <thread>
#include <cstring>
#include <librdkafka/rdkafka.h>

QueueProducer::QueueProducer(const IngesterConfig& config)
    : config_(config), in_flight_count_(0)
    , producer_(nullptr), topic_(nullptr), delivery_cb_(&in_flight_count_)
{
}

QueueProducer::~QueueProducer() {
    shutdown();
}

bool QueueProducer::initialize() {
    try {
        char errstr[512];
        
        // Create Kafka configuration
        rd_kafka_conf_t* conf = rd_kafka_conf_new();
        
        // Set brokers
        if (rd_kafka_conf_set(conf, "bootstrap.servers", config_.queue_brokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            std::cerr << "Failed to set bootstrap.servers: " << errstr << std::endl;
            rd_kafka_conf_destroy(conf);
            return false;
        }
        
        // Set acks
        std::string acks_str = std::to_string(config_.acks);
        if (rd_kafka_conf_set(conf, "acks", acks_str.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            std::cerr << "Failed to set acks: " << errstr << std::endl;
            rd_kafka_conf_destroy(conf);
            return false;
        }
        
        // Set compression type
        if (rd_kafka_conf_set(conf, "compression.type", config_.compression_type.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            std::cerr << "Failed to set compression.type: " << errstr << std::endl;
            rd_kafka_conf_destroy(conf);
            return false;
        }
        
        // Set retry backoff
        std::string retry_backoff_str = std::to_string(config_.retry_backoff_ms);
        if (rd_kafka_conf_set(conf, "retry.backoff.ms", retry_backoff_str.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            std::cerr << "Failed to set retry.backoff.ms: " << errstr << std::endl;
            rd_kafka_conf_destroy(conf);
            return false;
        }
        
        // Set queue buffering max messages
        std::string max_messages_str = std::to_string(config_.max_in_flight);
        if (rd_kafka_conf_set(conf, "queue.buffering.max.messages", max_messages_str.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            std::cerr << "Failed to set queue.buffering.max.messages: " << errstr << std::endl;
            rd_kafka_conf_destroy(conf);
            return false;
        }
        
        // Set queue buffering max kbytes (1GB = 1048576 KB)
        if (rd_kafka_conf_set(conf, "queue.buffering.max.kbytes", "1048576", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            std::cerr << "Failed to set queue.buffering.max.kbytes: " << errstr << std::endl;
            rd_kafka_conf_destroy(conf);
            return false;
        }
        
        // Set batch size
        if (rd_kafka_conf_set(conf, "batch.num.messages", "1000", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            std::cerr << "Failed to set batch.num.messages: " << errstr << std::endl;
            rd_kafka_conf_destroy(conf);
            return false;
        }
        
        // Set linger time
        if (rd_kafka_conf_set(conf, "linger.ms", "10", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            std::cerr << "Failed to set linger.ms: " << errstr << std::endl;
            rd_kafka_conf_destroy(conf);
            return false;
        }
        
        // Enable idempotence
        if (rd_kafka_conf_set(conf, "enable.idempotence", "true", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            std::cerr << "Failed to set enable.idempotence: " << errstr << std::endl;
            rd_kafka_conf_destroy(conf);
            return false;
        }
        
        // Set delivery report callback
        rd_kafka_conf_set_dr_cb(conf, DeliveryReportCb::dr_cb);
        rd_kafka_conf_set_opaque(conf, &delivery_cb_);
        
        // Create producer
        producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!producer_) {
            std::cerr << "Failed to create producer: " << errstr << std::endl;
            rd_kafka_conf_destroy(conf);
            return false;
        }
        
        // Create topic object
        rd_kafka_topic_conf_t* topic_conf = rd_kafka_topic_conf_new();
        topic_ = rd_kafka_topic_new(producer_, config_.queue_topic.c_str(), topic_conf);
        if (!topic_) {
            std::cerr << "Failed to create topic object: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
            rd_kafka_destroy(producer_);
            producer_ = nullptr;
            return false;
        }
        
        std::cout << "QueueProducer initialized with brokers: " << config_.queue_brokers 
                  << ", topic: " << config_.queue_topic << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to initialize QueueProducer: " << e.what() << std::endl;
        return false;
    }
}

ProduceResult QueueProducer::produce(
    const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request) {
    
    // Check backpressure
    if (isAtCapacity()) {
        return ProduceResult::QUEUE_FULL;
    }

    try {
        // Increment in-flight count before producing
        in_flight_count_.fetch_add(1);
        
        // Serialize the request
        std::string serialized = serializeRequest(request);
        
        // Produce with retry (will decrement counter on error)
        ProduceResult result = produceWithRetry(serialized, 0);
        
        return result;
    } catch (const std::exception& e) {
        std::cerr << "Error producing message: " << e.what() << std::endl;
        in_flight_count_.fetch_sub(1);
        return ProduceResult::PERSISTENT_ERROR;
    }
}

ProduceResult QueueProducer::produceWithRetry(const std::string& serialized_data, int retry_count) {
    try {
        // Produce message asynchronously
        int ret = rd_kafka_produce(
            topic_,
            RD_KAFKA_PARTITION_UA,  // Unassigned partition (let librdkafka choose)
            RD_KAFKA_MSG_F_COPY,    // Copy payload
            const_cast<void*>(static_cast<const void*>(serialized_data.data())),
            serialized_data.size(),
            nullptr,  // Key
            0,        // Key length
            nullptr   // Opaque (we use the global callback)
        );
        
        if (ret == -1) {
            rd_kafka_resp_err_t err = rd_kafka_last_error();
            
            if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                // Don't retry queue full errors - return immediately
                in_flight_count_.fetch_sub(1);
                return ProduceResult::QUEUE_FULL;
            }
            
            // Retry retryable errors
            if (retry_count < config_.max_retries && 
                (err == RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT ||
                 err == RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE ||
                 err == RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION)) {
                // Exponential backoff
                int backoff_ms = config_.retry_backoff_ms * (1 << retry_count);
                std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
                return produceWithRetry(serialized_data, retry_count + 1);
            }
            
            // Non-retryable error or max retries exceeded
            std::cerr << "Kafka error (attempt " << (retry_count + 1) << "/" << (config_.max_retries + 1) 
                      << "): " << rd_kafka_err2str(err) << std::endl;
            in_flight_count_.fetch_sub(1);
            return ProduceResult::PERSISTENT_ERROR;
        }
        
        // Message queued successfully - delivery callback will decrement in_flight_count_ on completion
        // Poll to ensure delivery callbacks are processed
        rd_kafka_poll(producer_, 0);
        
        return ProduceResult::SUCCESS;
    } catch (const std::exception& e) {
        std::cerr << "Unexpected error: " << e.what() << std::endl;
        in_flight_count_.fetch_sub(1);
        return ProduceResult::PERSISTENT_ERROR;
    }
}

std::string QueueProducer::serializeRequest(
    const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request) {
    std::string serialized;
    if (!request.SerializeToString(&serialized)) {
        throw std::runtime_error("Failed to serialize ExportLogsServiceRequest");
    }
    return serialized;
}

void QueueProducer::shutdown() {
    if (producer_) {
        try {
            // Flush any pending messages (wait up to 5 seconds)
            int remaining = rd_kafka_flush(producer_, 5000);
            if (remaining > 0) {
                std::cerr << "Warning: " << remaining << " messages were not delivered during shutdown" << std::endl;
            }
            
            // Poll to process any remaining delivery callbacks
            rd_kafka_poll(producer_, 0);
        } catch (const std::exception& e) {
            std::cerr << "Error during producer shutdown: " << e.what() << std::endl;
        }
        
        if (topic_) {
            rd_kafka_topic_destroy(topic_);
            topic_ = nullptr;
        }
        
        rd_kafka_destroy(producer_);
        producer_ = nullptr;
    }
}

