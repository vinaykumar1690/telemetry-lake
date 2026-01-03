#include "dead_letter_queue.hpp"
#include <iostream>
#include <iomanip>
#include <ctime>
#include <sstream>

DeadLetterQueue::DeadLetterQueue(const std::string& dlq_path)
    : dlq_path_(dlq_path), enabled_(!dlq_path.empty()) {
    if (enabled_) {
        initialize();
    }
}

DeadLetterQueue::~DeadLetterQueue() {
    if (dlq_file_ && dlq_file_->is_open()) {
        dlq_file_->close();
    }
}

bool DeadLetterQueue::initialize() {
    try {
        dlq_file_ = std::make_unique<std::ofstream>(dlq_path_, std::ios::app | std::ios::binary);
        if (!dlq_file_->is_open()) {
            std::cerr << "Failed to open dead letter queue file: " << dlq_path_ << std::endl;
            enabled_ = false;
            return false;
        }
        std::cout << "Dead letter queue initialized: " << dlq_path_ << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error initializing dead letter queue: " << e.what() << std::endl;
        enabled_ = false;
        return false;
    }
}

bool DeadLetterQueue::write(
    const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request,
    const std::string& error_reason) {
    
    if (!enabled_ || !dlq_file_ || !dlq_file_->is_open()) {
        return false;
    }
    
    std::lock_guard<std::mutex> lock(write_mutex_);
    
    try {
        // Write timestamp
        auto now = std::time(nullptr);
        std::tm* timeinfo = std::localtime(&now);
        *dlq_file_ << "[" << std::put_time(timeinfo, "%Y-%m-%d %H:%M:%S") << "] ";
        
        // Write error reason
        *dlq_file_ << "ERROR: " << error_reason << "\n";
        
        // Serialize and write the protobuf message
        std::string serialized;
        if (request.SerializeToString(&serialized)) {
            // Write length prefix
            uint32_t length = serialized.size();
            dlq_file_->write(reinterpret_cast<const char*>(&length), sizeof(length));
            // Write data
            dlq_file_->write(serialized.data(), serialized.size());
            *dlq_file_ << "\n---\n";
            dlq_file_->flush();
            return true;
        } else {
            std::cerr << "Failed to serialize message for DLQ" << std::endl;
            return false;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error writing to dead letter queue: " << e.what() << std::endl;
        return false;
    }
}

