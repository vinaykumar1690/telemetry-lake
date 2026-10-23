#include "ingester/http_server.hpp"
#include "ingester/ingester_metrics.hpp"
#include "ingester/queue_producer.hpp"
#include "config.hpp"
#include "observability/crow_log.hpp"
#include "observability/kafka_stats.hpp"
#include "observability/log.hpp"
#include "observability/metrics.hpp"
#include <iostream>
#include <memory>

int main() {
    try {
        obs::initLogging("otel-ingester");
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    obs::CrowLogHandler::install();

    IngesterConfig config;
    try {
        // Load configuration from environment
        config = IngesterConfig::fromEnv();
    } catch (const std::exception& e) {
        obs::error("main", "invalid configuration", {{"error", e.what()}});
        obs::error("main", "required: KAFKA_BROKERS (comma-separated broker addresses); "
                           "optional: KAFKA_TOPIC (default otel-logs)");
        return 1;
    }

    try {
        obs::Metrics metrics("otel-ingester");
        auto ingester_metrics = std::make_shared<IngesterMetrics>(metrics.registry());
        auto kafka_stats = std::make_shared<obs::KafkaStatsCollector>();
        metrics.addCollectable(kafka_stats);

        // Without a producer every accepted request would be dropped, so exit and
        // let the orchestrator restart us rather than serving traffic.
        auto queue_producer = std::make_shared<QueueProducer>(config, ingester_metrics, kafka_stats);
        if (!queue_producer->initialize()) {
            obs::error("main", "failed to initialize the Kafka producer");
            return 1;
        }
        metrics.addCollectable(std::make_shared<obs::CallbackCollectable>([queue_producer] {
            std::vector<prometheus::MetricFamily> out;
            out.push_back(obs::FamilyBuilder("ingester_kafka_in_flight_messages",
                                             "Messages produced and not yet acknowledged",
                                             prometheus::MetricType::Gauge)
                              .add(queue_producer->getInFlightCount())
                              .take());
            return out;
        }));

        // /metrics on its own port; the main app owns SIGINT/SIGTERM
        crow::SimpleApp metrics_app;
        CROW_ROUTE(metrics_app, "/metrics")([&metrics] {
            crow::response response(200, metrics.render());
            response.set_header("Content-Type", "text/plain; version=0.0.4");
            return response;
        });
        metrics_app.signal_clear();
        metrics_app.loglevel(crow::LogLevel::Warning);
        auto metrics_running = metrics_app.port(config.metrics_port).concurrency(1).run_async();
        obs::info("main", "metrics listening", {{"port", config.metrics_port}, {"path", "/metrics"}});

        HttpServerOptions options;
        options.host = config.http_host;
        options.port = config.http_port;
        options.threads = config.http_threads;
        options.max_body_bytes = config.max_body_bytes;
        options.max_decompressed_bytes = config.max_decompressed_bytes;

        HttpServer server(queue_producer, std::move(options), ingester_metrics);
        server.start();

        metrics_app.stop();
        metrics_running.wait();
        obs::info("main", "ingester stopped");
    } catch (const std::exception& e) {
        obs::error("main", "fatal error", {{"error", e.what()}});
        return 1;
    }
    return 0;
}
