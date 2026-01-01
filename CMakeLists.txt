cmake_minimum_required(VERSION 3.11)
project(otel_receiver)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)
FetchContent_Declare(
  Crow
  GIT_REPOSITORY https://github.com/CrowCpp/Crow.git
  GIT_TAG v1.3.0.0 # You can specify a tag, branch, or commit hash
)

FetchContent_MakeAvailable(Crow)

add_executable(otel_receiver src/main.cpp src/http_server.cpp)

target_link_libraries(otel_receiver PUBLIC Crow)

