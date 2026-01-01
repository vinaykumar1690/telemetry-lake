#include "http_server.hpp"

int main() {
    HttpServer server;
    server.start("0.0.0.0", 4318);
    return 0;
}
