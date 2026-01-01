# OTel Log Receiver

This is a simple C++ application that runs an HTTP server to receive OpenTelemetry (OTel) logs and print them to standard output.

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
