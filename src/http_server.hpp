#ifndef HTTP_SERVER_HPP
#define HTTP_SERVER_HPP

#include <string>

class HttpServer {
public:
    HttpServer();
    void start(const std::string& host, int port);
};

#endif // HTTP_SERVER_HPP
