#ifndef HTTP_SERVER_HPP
#define HTTP_SERVER_HPP

#include <string>
#include "crow.h"

class HttpServer {
public:
    HttpServer();
    void start(const std::string& host, int port);
    // Setup routes on the provided app (for testing)
    void setupRoutes(crow::SimpleApp& app);
};

#endif // HTTP_SERVER_HPP
