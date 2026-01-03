#ifndef HTTP_SERVER_HPP
#define HTTP_SERVER_HPP

#include <string>
#include <memory>
#include "crow.h"

class QueueProducer;

class HttpServer {
public:
    HttpServer();
    HttpServer(std::shared_ptr<QueueProducer> queue_producer);
    void start(const std::string& host, int port);
    // Setup routes on the provided app (for testing)
    void setupRoutes(crow::SimpleApp& app);
    
private:
    std::shared_ptr<QueueProducer> queue_producer_;
};

#endif // HTTP_SERVER_HPP

