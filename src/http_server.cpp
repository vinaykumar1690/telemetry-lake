#include "http_server.hpp"
#include "crow.h"
#include <iostream>

HttpServer::HttpServer() {}

void HttpServer::start(const std::string& host, int port) {
    crow::SimpleApp app;

    CROW_ROUTE(app, "/v1/logs")
        .methods("POST"_method)
        ([](const crow::request& req){
            std::cout << "Received OTel Log:" << std::endl;
            std::cout << req.body << std::endl;
            return crow::response(200, "OK");
        });

    std::cout << "OTel Log Receiver is running at http://" << host << ":" << port << std::endl;
    app.bindaddr(host).port(port).run();
}
