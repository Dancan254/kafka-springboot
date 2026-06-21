# Changelog

All notable changes to this project are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [Unreleased]

---

## [1.2.0] — 2026-06-22

### Fixed
- `opentelemetry-logback-appender-1.0` is no longer a transitive dependency of `spring-boot-starter-opentelemetry` in Spring Boot 4.x. Both `producer` and `consumer` now declare it explicitly with compile scope (version `2.29.0-alpha`) so that `InstallOpenTelemetryAppender` resolves at compile time and log signals reach Loki at runtime.

---

## [1.1.0] — 2026-06-20

### Added
- **3-broker KRaft cluster** — replaced single-broker setup with a 3-node KRaft cluster (no ZooKeeper). Each node acts as both broker and controller. Raft quorum uses internal port 29093.
- **Idempotent producer** — `enable.idempotence=true` with `acks=all`, `retries=3`, and `max.in.flight.requests.per.connection=5`. Snappy compression and 32 KiB batches with `linger.ms=20`.
- **Topic config** — `wikimedia-stream` declared with 3 partitions, RF=3, `min.insync.replicas=2`, 7-day retention, and 10 GiB/partition size cap.
- **Manual acknowledgment** — consumer uses `ack-mode=MANUAL_IMMEDIATE` and `enable-auto-commit=false`. Offset advances only after the record is persisted to H2.
- **Dead-Letter Topic (DLT)** — `DefaultErrorHandler` with `ExponentialBackOffWithMaxRetries` (1 s → 2 s → 4 s, max 3 retries) routes exhausted records to `wikimedia-stream.dlt` on the same partition. `IllegalArgumentException` is non-retryable and skips straight to the DLT.
- **`WikimediaDltConsumer`** — dedicated listener on `wikimedia-consumer-group-dlt` that logs all Spring Kafka diagnostic headers (original topic, partition, offset, exception class and message).
- **`WikimediaEvent` JPA entity** — persists Kafka provenance columns (`kafkaPartition`, `kafkaOffset`, `processedAt`) alongside the parsed Wikimedia fields.
- **REST API** — `WikimediaEventController` exposes `/events`, `/events/recent`, `/events/wiki/{wiki}`, `/events/type/{type}`, and `/events/stats` on port 8082.
- **`read_committed` isolation** — consumer ignores messages from uncommitted transactions, preventing half-written transactional reads.
- **Full observability stack** — OTel Collector, Prometheus, Grafana Tempo, Grafana Loki, and Grafana added to `docker-compose.yml`. Both Spring Boot apps push metrics, traces, and logs over OTLP HTTP to the collector.
- **`InstallOpenTelemetryAppender`** — Spring `InitializingBean` that wires the fully-constructed `OpenTelemetry` SDK bean into the Logback `OpenTelemetryAppender` after the application context is ready, enabling log-to-Loki shipping.
- **`logback-spring.xml`** — added to both modules; routes all `INFO`+ logs to both the console appender and the OTel appender.
- **`kafka-exporter`** — added to Docker Compose; exposes consumer group lag and topic partition offsets as Prometheus metrics (scraped at `:9308`).
- **`tempo-config.yml`** — local storage config for Grafana Tempo.
- **`otel-collector-config.yml`** — three pipelines: metrics → Prometheus exporter, traces → Tempo, logs → Loki.
- **`prometheus.yml`** — scrape config for `kafka-exporter` and the OTel Collector Prometheus exporter.
- **Comprehensive README** — architecture diagram, Kafka concept deep-dives, step-by-step getting started guide, full configuration reference, PromQL/LogQL examples, and production checklist.

### Changed
- `docker-compose.yml` expanded from a single-broker setup to the full production-grade stack (3 Kafka brokers + observability services).
- `consumer/pom.xml` — added `spring-boot-starter-actuator`, `spring-boot-starter-opentelemetry`, and `spring-boot-starter-data-jpa` (H2 runtime).
- `producer/pom.xml` — added `spring-boot-starter-actuator` and `spring-boot-starter-opentelemetry`.
- Consumer upgraded from `@KafkaListener` with auto-commit to a fully configured `ConcurrentKafkaListenerContainerFactory` with custom error handling.

---

## [1.0.0] — 2025-11-17

### Added
- Initial project scaffold — Maven multi-module layout with `producer` and `consumer` Spring Boot 4 modules.
- Single-broker Kafka setup via Docker Compose.
- `WikimediaStreamConsumer` — WebClient/WebFlux reactive SSE client that reads the Wikimedia Recent Changes stream (`stream.wikimedia.org/v2/stream/recentchange`).
- `WikimediaProducer` — `KafkaTemplate`-based producer with send callback logging.
- `WikimediaController` — `GET /api/v1/wikimedia` triggers the SSE stream.
- Basic `application.yml` configs for both modules.
