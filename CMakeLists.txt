cmake_minimum_required(VERSION 3.11)
project(otel_receiver)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

# Fetch Crow web framework
FetchContent_Declare(
  Crow
  GIT_REPOSITORY https://github.com/CrowCpp/Crow.git
  GIT_TAG v1.3.0.0
)

# Fetch Protocol Buffers
FetchContent_Declare(
  protobuf
  GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
  GIT_TAG v25.3
)

# Fetch OpenTelemetry Protocol Buffers definitions
FetchContent_Declare(
  opentelemetry_proto
  GIT_REPOSITORY https://github.com/open-telemetry/opentelemetry-proto.git
  GIT_TAG v1.3.1
)

# Configure protobuf before making it available
set(protobuf_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(protobuf_BUILD_PROTOC_BINARIES ON CACHE BOOL "" FORCE)

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

# Fetch Google Test
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.14.0
)

# Make dependencies available
FetchContent_MakeAvailable(Crow protobuf opentelemetry_proto googletest)

# Set up OpenTelemetry protobuf compilation
set(OPENTELEMETRY_PROTO_ROOT ${opentelemetry_proto_SOURCE_DIR})

# Generate protobuf C++ files
# We need to compile the proto files in dependency order
set(OTEL_PROTO_FILES
  "${OPENTELEMETRY_PROTO_ROOT}/opentelemetry/proto/common/v1/common.proto"
  "${OPENTELEMETRY_PROTO_ROOT}/opentelemetry/proto/resource/v1/resource.proto"
  "${OPENTELEMETRY_PROTO_ROOT}/opentelemetry/proto/logs/v1/logs.proto"
  "${OPENTELEMETRY_PROTO_ROOT}/opentelemetry/proto/collector/logs/v1/logs_service.proto"
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

# Create a library for the generated protobuf files
add_library(otel_proto STATIC ${OTEL_PROTO_GENERATED_SRCS} ${OTEL_PROTO_GENERATED_HDRS})
target_link_libraries(otel_proto PUBLIC protobuf::libprotobuf)
target_include_directories(otel_proto PUBLIC 
  ${CMAKE_CURRENT_BINARY_DIR}
  ${OPENTELEMETRY_PROTO_ROOT}
)

# Create executable
add_executable(otel_receiver src/main.cpp src/http_server.cpp)

# Link libraries
target_link_libraries(otel_receiver PUBLIC 
  Crow
  protobuf::libprotobuf
  otel_proto
  ZLIB::ZLIB
)

# Include directories for protobuf JSON utilities
target_include_directories(otel_receiver PRIVATE
  ${protobuf_SOURCE_DIR}/src
)

# Create test executable
enable_testing()
add_executable(http_server_test tests/test_http_server.cpp src/http_server.cpp)

# Link libraries for test
target_link_libraries(http_server_test PRIVATE
  GTest::gtest
  GTest::gtest_main
  GTest::gmock
  Crow
  protobuf::libprotobuf
  otel_proto
  ZLIB::ZLIB
)

# Include directories for test
target_include_directories(http_server_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${protobuf_SOURCE_DIR}/src
)

# Add test to CTest
add_test(NAME HttpServerTest COMMAND http_server_test)

