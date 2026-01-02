# OTel Log Receiver

This is a C++ application that runs an HTTP server to receive OpenTelemetry (OTel) logs from an OpenTelemetry logs agent and writes the logs data to an Iceberg table.

The HTTP server listens on port `4318`, which is the default port for OTLP/HTTP.

## Dependencies

*   A C++17 compatible compiler (e.g., g++, clang++)
*   CMake (version 3.10 or later)

## How to Build

1.  **Create a build directory:**
    ```bash
    mkdir build
    cd build
    ```

2.  **Run CMake to configure the project:**
    ```bash
    cmake ..
    ```

3.  **Compile the project:**
    ```bash
    make
    ```

## How to Run

After building, you can run the receiver from the `build` directory:

```bash
./otel_receiver
```

You will see the following output, indicating the server is running:

```
OTel Log Receiver is running at http://0.0.0.0:4318
```

## How to Send Logs

You can send OTel logs to the receiver using a `curl` command. The endpoint is `/v1/logs`.

Here is an example of sending a simple JSON log payload:

```bash
curl -X POST -H "Content-Type: application/json" -d '{
  "resourceLogs": [
    {
      "resource": {
        "attributes": [
          {
            "key": "service.name",
            "value": {
              "stringValue": "my-test-service"
            }
          }
        ]
      },
      "scopeLogs": [
        {
          "scope": {},
          "logRecords": [
            {
              "timeUnixNano": "1672531200000000000",
              "severityText": "INFO",
              "body": {
                "stringValue": "This is a log message."
              }
            }
          ]
        }
      ]
    }
  ]
}' http://localhost:4318/v1/logs
```

When the server receives the log, it will print the JSON payload to the console where `otel_receiver` is running.

## Running Tests

The project includes unit tests using Google Test (gtest). To build and run the test suite:

1. **Build the test executable:**
   ```bash
   cd build
   make http_server_test
   ```

   Or if using ninja:
   ```bash
   cd build
   ninja http_server_test
   ```

2. **Run the tests:**
   ```bash
   ./http_server_test
   ```

   Or use CTest:
   ```bash
   ctest -R HttpServerTest
   ```

The test suite covers the `/v1/logs` endpoint implementation, including:
- Handling of protobuf and JSON content types
- Gzip-encoded request decompression
- Error handling for invalid payloads and unsupported content types
