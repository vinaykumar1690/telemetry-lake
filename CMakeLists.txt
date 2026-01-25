cmake_minimum_required(VERSION 3.11)
project(otel_receiver)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

# Find Crow web framework (header-only, install via: brew install crow)
# Note: Crow 1.3.0 is expected but CrowConfig.cmake doesn't export version info
find_package(Crow REQUIRED)

# Fetch Protocol Buffers
FetchContent_Declare(
  protobuf
  GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
  GIT_TAG v25.3
)

# Configure protobuf before making it available
set(protobuf_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(protobuf_BUILD_PROTOC_BINARIES ON CACHE BOOL "" FORCE)

# Fetch OpenTelemetry Protocol Buffers definitions
FetchContent_Declare(
  opentelemetry_proto
  GIT_REPOSITORY https://github.com/open-telemetry/opentelemetry-proto.git
  GIT_TAG v1.3.1
)


# Find or fetch zlib for gzip decompression
find_package(ZLIB QUIET)
if(NOT ZLIB_FOUND)
  # Fetch zlib if not found on system
  FetchContent_Declare(
    zlib
    GIT_REPOSITORY https://github.com/madler/zlib.git
    GIT_TAG v1.3.1
  )
  FetchContent_MakeAvailable(zlib)
  # Create alias to match find_package target name
  # zlib typically creates 'zlibstatic' for static builds
  if(TARGET zlibstatic AND NOT TARGET ZLIB::ZLIB)
    add_library(ZLIB::ZLIB ALIAS zlibstatic)
  elseif(TARGET zlib AND NOT TARGET ZLIB::ZLIB)
    add_library(ZLIB::ZLIB ALIAS zlib)
  endif()
endif()

# Find Google Test (install via: brew install googletest)
# Note: GoogleTest 1.17.0 expected from system
find_package(GTest REQUIRED)

# Find librdkafka for Kafka client (required)
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(RDKAFKA rdkafka)
endif()

# Always try to find the library directly to get full path
find_library(RDKAFKA_LIBRARY
  NAMES rdkafka
  PATHS
    /usr/lib
    /usr/local/lib
    /opt/homebrew/lib
    ${RDKAFKA_LIBRARY_DIRS}
  NO_DEFAULT_PATH
)
if(NOT RDKAFKA_LIBRARY)
  find_library(RDKAFKA_LIBRARY
    NAMES rdkafka
    PATHS
      /usr/lib
      /usr/local/lib
      /opt/homebrew/lib
  )
endif()

# Find include directory
if(RDKAFKA_FOUND AND RDKAFKA_INCLUDE_DIRS)
  set(RDKAFKA_INCLUDE_DIR ${RDKAFKA_INCLUDE_DIRS})
else()
  find_path(RDKAFKA_INCLUDE_DIR
    NAMES librdkafka/rdkafka.h
    PATHS
      /usr/include
      /usr/local/include
      /opt/homebrew/include
      ${RDKAFKA_INCLUDE_DIRS}
    NO_DEFAULT_PATH
  )
  if(NOT RDKAFKA_INCLUDE_DIR)
    find_path(RDKAFKA_INCLUDE_DIR
      NAMES librdkafka/rdkafka.h
      PATHS
        /usr/include
        /usr/local/include
        /opt/homebrew/include
    )
  endif()
endif()

# Create imported target if we found the library
if(RDKAFKA_LIBRARY AND RDKAFKA_INCLUDE_DIR)
  if(NOT TARGET rdkafka::rdkafka)
    add_library(rdkafka::rdkafka UNKNOWN IMPORTED)
    set_target_properties(rdkafka::rdkafka PROPERTIES
      IMPORTED_LOCATION "${RDKAFKA_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${RDKAFKA_INCLUDE_DIR}"
    )
  endif()
  set(RDKAFKA_FOUND TRUE)
endif()

# Fail if librdkafka is not found
if(NOT RDKAFKA_FOUND)
  message(FATAL_ERROR "librdkafka is required but not found. Please install it via: brew install librdkafka (macOS) or apt-get install librdkafka-dev (Linux)")
endif()

# Fetch cppkafka (C++ wrapper for librdkafka)
FetchContent_Declare(
  cppkafka
  GIT_REPOSITORY https://github.com/mfontanini/cppkafka.git
  GIT_TAG v0.3.1
)

# Make dependencies available (librdkafka must be found before cppkafka)
FetchContent_MakeAvailable(protobuf opentelemetry_proto cppkafka)

# Find pre-built DuckDB library
# DuckDB binary distribution should be installed to /usr/local or specified via DUCKDB_ROOT
set(DUCKDB_ROOT "" CACHE PATH "Path to DuckDB installation")

# Search for DuckDB library
find_library(DUCKDB_LIBRARY
  NAMES duckdb
  PATHS
    ${DUCKDB_ROOT}/lib
    ${DUCKDB_ROOT}
    /usr/local/lib
    /usr/lib
    /opt/homebrew/lib
)

# Search for DuckDB headers
find_path(DUCKDB_INCLUDE_DIR
  NAMES duckdb.hpp
  PATHS
    ${DUCKDB_ROOT}/include
    ${DUCKDB_ROOT}
    /usr/local/include
    /usr/include
    /opt/homebrew/include
)

if(NOT DUCKDB_LIBRARY OR NOT DUCKDB_INCLUDE_DIR)
  message(WARNING "DuckDB library not found. otel_appender target will not be built. "
    "To build appender, install libduckdb or set DUCKDB_ROOT. "
    "Download from: https://github.com/duckdb/duckdb/releases")
  set(DUCKDB_FOUND FALSE)
else()
  set(DUCKDB_FOUND TRUE)
  message(STATUS "Found DuckDB library: ${DUCKDB_LIBRARY}")
  message(STATUS "Found DuckDB headers: ${DUCKDB_INCLUDE_DIR}")

  # Create imported target for DuckDB
  add_library(duckdb SHARED IMPORTED)
  set_target_properties(duckdb PROPERTIES
    IMPORTED_LOCATION "${DUCKDB_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${DUCKDB_INCLUDE_DIR}"
  )
endif()

# Set up OpenTelemetry protobuf compilation
set(OPENTELEMETRY_PROTO_ROOT ${opentelemetry_proto_SOURCE_DIR})

# Set up custom proto root for our wrapper messages
set(CUSTOM_PROTO_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/proto)

# Generate protobuf C++ files
# We need to compile the proto files in dependency order
set(OTEL_PROTO_FILES
  "${OPENTELEMETRY_PROTO_ROOT}/opentelemetry/proto/common/v1/common.proto"
  "${OPENTELEMETRY_PROTO_ROOT}/opentelemetry/proto/resource/v1/resource.proto"
  "${OPENTELEMETRY_PROTO_ROOT}/opentelemetry/proto/logs/v1/logs.proto"
  "${OPENTELEMETRY_PROTO_ROOT}/opentelemetry/proto/collector/logs/v1/logs_service.proto"
)

# Custom proto files (telemetry wrapper)
set(CUSTOM_PROTO_FILES
  "${CUSTOM_PROTO_ROOT}/telemetry_wrapper.proto"
)

set(OTEL_PROTO_GENERATED_SRCS)
set(OTEL_PROTO_GENERATED_HDRS)

foreach(proto_file ${OTEL_PROTO_FILES})
  get_filename_component(proto_name ${proto_file} NAME_WE)
  get_filename_component(proto_path ${proto_file} PATH)
  
  # Calculate relative path from OPENTELEMETRY_PROTO_ROOT
  file(RELATIVE_PATH rel_path ${OPENTELEMETRY_PROTO_ROOT} ${proto_path})
  
  set(src_file ${CMAKE_CURRENT_BINARY_DIR}/${rel_path}/${proto_name}.pb.cc)
  set(hdr_file ${CMAKE_CURRENT_BINARY_DIR}/${rel_path}/${proto_name}.pb.h)
  
  list(APPEND OTEL_PROTO_GENERATED_SRCS ${src_file})
  list(APPEND OTEL_PROTO_GENERATED_HDRS ${hdr_file})
  
  # Create output directory
  get_filename_component(out_dir ${src_file} PATH)
  file(MAKE_DIRECTORY ${out_dir})
  
  # Generate protobuf files using generator expression for protoc executable
  if(TARGET protobuf::protoc)
    add_custom_command(
      OUTPUT ${src_file} ${hdr_file}
      COMMAND $<TARGET_FILE:protobuf::protoc>
      ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
           --proto_path=${OPENTELEMETRY_PROTO_ROOT}
           ${proto_file}
      DEPENDS ${proto_file} protobuf::protoc
      COMMENT "Generating ${proto_name}.pb.cc and ${proto_name}.pb.h"
    )
  elseif(TARGET protoc)
    add_custom_command(
      OUTPUT ${src_file} ${hdr_file}
      COMMAND $<TARGET_FILE:protoc>
      ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
           --proto_path=${OPENTELEMETRY_PROTO_ROOT}
           ${proto_file}
      DEPENDS ${proto_file} protoc
      COMMENT "Generating ${proto_name}.pb.cc and ${proto_name}.pb.h"
    )
  else()
    # Fallback: try to find protoc
    find_program(PROTOC_EXECUTABLE 
      NAMES protoc
      PATHS ${protobuf_BINARY_DIR}
      NO_DEFAULT_PATH
    )
    if(NOT PROTOC_EXECUTABLE)
      find_program(PROTOC_EXECUTABLE protoc REQUIRED)
    endif()
    add_custom_command(
      OUTPUT ${src_file} ${hdr_file}
      COMMAND ${PROTOC_EXECUTABLE}
      ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
           --proto_path=${OPENTELEMETRY_PROTO_ROOT}
           ${proto_file}
      DEPENDS ${proto_file}
      COMMENT "Generating ${proto_name}.pb.cc and ${proto_name}.pb.h"
    )
  endif()
endforeach()

# Generate custom proto files (telemetry wrapper)
set(CUSTOM_PROTO_GENERATED_SRCS)
set(CUSTOM_PROTO_GENERATED_HDRS)

foreach(proto_file ${CUSTOM_PROTO_FILES})
  get_filename_component(proto_name ${proto_file} NAME_WE)

  set(src_file ${CMAKE_CURRENT_BINARY_DIR}/${proto_name}.pb.cc)
  set(hdr_file ${CMAKE_CURRENT_BINARY_DIR}/${proto_name}.pb.h)

  list(APPEND CUSTOM_PROTO_GENERATED_SRCS ${src_file})
  list(APPEND CUSTOM_PROTO_GENERATED_HDRS ${hdr_file})

  # Generate protobuf files
  if(TARGET protobuf::protoc)
    add_custom_command(
      OUTPUT ${src_file} ${hdr_file}
      COMMAND $<TARGET_FILE:protobuf::protoc>
      ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
           --proto_path=${CUSTOM_PROTO_ROOT}
           ${proto_file}
      DEPENDS ${proto_file} protobuf::protoc
      COMMENT "Generating ${proto_name}.pb.cc and ${proto_name}.pb.h"
    )
  elseif(TARGET protoc)
    add_custom_command(
      OUTPUT ${src_file} ${hdr_file}
      COMMAND $<TARGET_FILE:protoc>
      ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
           --proto_path=${CUSTOM_PROTO_ROOT}
           ${proto_file}
      DEPENDS ${proto_file} protoc
      COMMENT "Generating ${proto_name}.pb.cc and ${proto_name}.pb.h"
    )
  else()
    add_custom_command(
      OUTPUT ${src_file} ${hdr_file}
      COMMAND ${PROTOC_EXECUTABLE}
      ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
           --proto_path=${CUSTOM_PROTO_ROOT}
           ${proto_file}
      DEPENDS ${proto_file}
      COMMENT "Generating ${proto_name}.pb.cc and ${proto_name}.pb.h"
    )
  endif()
endforeach()

# Create a library for the generated protobuf files (including custom protos)
add_library(otel_proto STATIC
  ${OTEL_PROTO_GENERATED_SRCS}
  ${OTEL_PROTO_GENERATED_HDRS}
  ${CUSTOM_PROTO_GENERATED_SRCS}
  ${CUSTOM_PROTO_GENERATED_HDRS}
)
target_link_libraries(otel_proto PUBLIC protobuf::libprotobuf)
target_include_directories(otel_proto PUBLIC
  ${CMAKE_CURRENT_BINARY_DIR}
  ${OPENTELEMETRY_PROTO_ROOT}
)

# Create executable
add_executable(otel_receiver src/main.cpp src/ingester/http_server.cpp src/config.cpp src/ingester/queue_producer.cpp)

# Link libraries
target_link_libraries(otel_receiver PUBLIC
  Crow::Crow
  protobuf::libprotobuf
  otel_proto
  ZLIB::ZLIB
)

# Link librdkafka (required)
target_link_libraries(otel_receiver PUBLIC rdkafka::rdkafka)

# Include directories for protobuf JSON utilities
target_include_directories(otel_receiver PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/src/ingester
  ${protobuf_SOURCE_DIR}/src
)

# Create test executable
enable_testing()
add_executable(http_server_test 
  tests/test_http_server.cpp 
  src/ingester/http_server.cpp 
  src/ingester/queue_producer.cpp
  src/config.cpp
)

# Link libraries for test
target_link_libraries(http_server_test PRIVATE
  GTest::gtest
  GTest::gtest_main
  GTest::gmock
  Crow::Crow
  protobuf::libprotobuf
  otel_proto
  ZLIB::ZLIB
  rdkafka::rdkafka
)

# Include directories for test
target_include_directories(http_server_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/src/ingester
  ${protobuf_SOURCE_DIR}/src
)

# Add test to CTest
add_test(NAME HttpServerTest COMMAND http_server_test)

# Create buffer manager test
add_executable(buffer_manager_test tests/test_buffer_manager.cpp src/appender/buffer_manager.cpp)
target_link_libraries(buffer_manager_test PRIVATE GTest::gtest GTest::gtest_main)
target_include_directories(buffer_manager_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
add_test(NAME BufferManagerTest COMMAND buffer_manager_test)

# Create log transformer test
add_executable(log_transformer_test 
  tests/test_log_transformer.cpp 
  src/appender/log_transformer.cpp
)
target_link_libraries(log_transformer_test PRIVATE 
  GTest::gtest 
  GTest::gtest_main 
  protobuf::libprotobuf
  otel_proto
)
target_include_directories(log_transformer_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${protobuf_SOURCE_DIR}/src
)
add_test(NAME LogTransformerTest COMMAND log_transformer_test)

# Create appender executable (only if DuckDB is found)
if(DUCKDB_FOUND)
  add_executable(otel_appender
    src/appender/main.cpp
    src/appender/queue_consumer.cpp
    src/appender/log_transformer.cpp
    src/appender/iceberg_appender.cpp
    src/appender/buffer_manager.cpp
    src/appender/dead_letter_queue.cpp
    src/config.cpp
  )

  # Link libraries for appender
  target_link_libraries(otel_appender PRIVATE
    protobuf::libprotobuf
    otel_proto
    duckdb
    cppkafka
    rdkafka::rdkafka
  )

  # Include directories for appender
  target_include_directories(otel_appender PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${protobuf_SOURCE_DIR}/src
    ${DUCKDB_INCLUDE_DIR}
    ${cppkafka_SOURCE_DIR}/include
  )
endif()

