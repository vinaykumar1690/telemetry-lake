#ifndef DEAD_LETTER_QUEUE_HPP
#define DEAD_LETTER_QUEUE_HPP

#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"
#include <string>
#include <fstream>
#include <mutex>
#include <memory>

// Dead letter queue for messages that fail after all retries
class DeadLetterQueue {
public:
    DeadLetterQueue(const std::string& dlq_path);
    ~DeadLetterQueue();
    
    // Write a failed message to the DLQ
    bool write(const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request,
               const std::string& error_reason);
    
    // Check if DLQ is enabled
    bool isEnabled() const { return enabled_; }

private:
    std::string dlq_path_;
    bool enabled_;
    std::unique_ptr<std::ofstream> dlq_file_;
    std::mutex write_mutex_;
    
    bool initialize();
};

#endif // DEAD_LETTER_QUEUE_HPP

