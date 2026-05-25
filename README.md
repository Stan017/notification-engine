# Distributed Notification Engine

> Event-driven notification microservice built with **Java 21**, **Spring Boot 3**, **Apache Kafka** and **PostgreSQL 16**.  
> Designed to mirror the architecture patterns used at companies like Mercado Libre for high-throughput, resilient notification pipelines.

[![CI](https://github.com/Stan017/notification-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/Stan017/notification-engine/actions/workflows/ci.yml)
![Java](https://img.shields.io/badge/Java-21-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.5-brightgreen)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7.1-black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        notification-engine                          │
│                                                                     │
│  ┌──────────────────┐        ┌──────────────────────────────────┐  │
│  │  notification-   │        │     notification-consumer        │  │
│  │    producer      │        │                                  │  │
│  │                  │        │  ┌────────────────────────────┐  │  │
│  │  REST API        │        │  │   DomainEventListener      │  │  │
│  │  POST /events    │        │  │   @KafkaListener           │  │  │
│  │                  │        │  └─────────────┬──────────────┘  │  │
│  │  EventPublisher  │        │                │                  │  │
│  │  Service         │        │  ┌─────────────▼──────────────┐  │  │
│  └────────┬─────────┘        │  │  NotificationRouterService  │  │  │
│           │                  │  │  resolveChannels(eventType) │  │  │
│           │ KafkaTemplate    │  └─────────────┬──────────────┘  │  │
│           │                  │                │                  │  │
│  ┌────────▼─────────┐        │  ┌─────────────▼──────────────┐  │  │
│  │   Apache Kafka   │        │  │     ChannelAdapter          │  │  │
│  │                  │        │  │  EMAIL | PUSH | SMS         │  │  │
│  │  ml.orders       ◄────────►  └─────────────┬──────────────┘  │  │
│  │  ml.payments     │        │                │                  │  │
│  │  ml.shipments    │        │  ┌─────────────▼──────────────┐  │  │
│  │  ml.notifications│        │  │       PostgreSQL 16         │  │  │
│  │    .dlq          │        │  │  notifications table        │  │  │
│  └──────────────────┘        │  │  notification_events        │  │  │
│                               │  └────────────────────────────┘  │  │
│                               └──────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## Event Flow

```
Producer                    Kafka                      Consumer
   │                          │                           │
   │  POST /api/v1/events      │                           │
   │─────────────────────►    │                           │
   │                          │   DomainEvent published   │
   │                          │──────────────────────────►│
   │                          │                           │ Idempotency check
   │                          │                           │ (notification_events)
   │                          │                           │
   │                          │                           │ resolveChannels()
   │                          │                           │ EMAIL + PUSH / SMS
   │                          │                           │
   │                          │                           │ ChannelAdapter.send()
   │                          │                           │ markSent() → DB
   │                          │                           │
   │                          │  [on failure x3]          │
   │                          │◄──────────────────────────│
   │                          │  → DLQ topic              │
```

## DLQ & Retry Flow

```
Consumer (failure)
      │
      │ 3 retries × 2s backoff (FixedBackOff)
      │ DefaultErrorHandler + DeadLetterPublishingRecoverer
      ▼
ml.notifications.dlq (Kafka topic)
      │
      │ Admin API (manual inspection)
      ▼
GET  /api/v1/admin/dlq          → list failed notifications
POST /api/v1/admin/dlq/{id}/retry → re-dispatch via original channel
```

---

## Project Structure

```
notification-engine/                  ← Maven parent (multi-module)
├── notification-common/              ← Shared DTOs, enums, interfaces
│   └── src/main/java/
│       └── com/ml/notification/common/
│           ├── dto/DomainEvent.java
│           └── enums/EventType, Channel, NotificationStatus
│
├── notification-producer/            ← REST API (port 8080)
│   └── src/main/java/
│       └── com/ml/notification/producer/
│           ├── controller/EventController.java
│           ├── service/EventPublisherService.java
│           └── config/KafkaProducerConfig.java
│
└── notification-consumer/            ← Kafka consumer + Admin API (port 8081)
    └── src/main/java/
        └── com/ml/notification/consumer/
            ├── listener/DomainEventListener.java
            ├── service/
            │   ├── NotificationRouterService.java
            │   ├── NotificationStatusService.java
            │   └── IdempotencyService.java
            ├── channel/
            │   ├── ChannelAdapter.java          ← interface
            │   ├── EmailChannelAdapter.java
            │   ├── PushChannelAdapter.java
            │   └── SmsChannelAdapter.java
            ├── admin/
            │   ├── controller/AdminController.java
            │   └── service/AdminService.java
            ├── entity/Notification.java
            └── config/KafkaConsumerConfig.java
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 21 (switch expressions, records) |
| Framework | Spring Boot 3.3.5 |
| Messaging | Apache Kafka 3.7.1 (KRaft — no Zookeeper) |
| Persistence | Spring Data JPA + Hibernate + PostgreSQL 16 |
| Migrations | Flyway 10.x |
| API Docs | SpringDoc OpenAPI 3 (Swagger UI) |
| Resilience | Spring Retry + AOP, DLQ with FixedBackOff |
| Testing | JUnit 5, Mockito, AssertJ, Awaitility |
| Build | Maven 3.9 (multi-module) |
| CI/CD | GitHub Actions |
| Infra (local) | Docker Compose |

---

## Supported Events & Channels

| Event Type | Channels |
|---|---|
| `ORDER_CREATED` | EMAIL + PUSH |
| `ORDER_SHIPPED` | PUSH + SMS |
| `ORDER_DELIVERED` | EMAIL + PUSH |
| `PAYMENT_APPROVED` | EMAIL |
| `PAYMENT_REJECTED` | EMAIL + PUSH |
| `SHIPMENT_UPDATED` | PUSH |

---

## Getting Started

### Prerequisites

- Java 21
- Maven 3.9+
- Docker Desktop

### 1. Start infrastructure

```bash
docker-compose up -d
```

This starts:
- **Apache Kafka** on `localhost:9092` (KRaft mode, no Zookeeper)
- **PostgreSQL 16** on `localhost:5433` (user: `mluser`, pass: `mlpass`, db: `notifications`)

### 2. Build the project

```bash
mvn clean install -DskipTests
```

### 3. Start the Producer (port 8080)

```bash
mvn spring-boot:run -pl notification-producer
```

### 4. Start the Consumer (port 8081)

```bash
mvn spring-boot:run -pl notification-consumer
```

### 5. Explore the APIs

| Service | Swagger UI |
|---|---|
| Producer | http://localhost:8080/swagger-ui.html |
| Consumer / Admin | http://localhost:8081/swagger-ui.html |

---

## API Quick Reference

### Producer — Publish an event

```bash
curl -X POST http://localhost:8080/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "ORDER_CREATED",
    "userId": "usr_123",
    "payload": { "orderId": "ORD-9999", "total": 4500.00 }
  }'
```

### Admin — List failed notifications (DLQ)

```bash
curl http://localhost:8081/api/v1/admin/dlq
```

### Admin — Retry a failed notification

```bash
curl -X POST http://localhost:8081/api/v1/admin/dlq/{id}/retry
```

### Admin — Filter notifications by status

```bash
curl "http://localhost:8081/api/v1/admin/notifications?status=FAILED"
```

---

## Running Tests

### Unit tests only

```bash
mvn test -pl notification-consumer -am
```

### Full suite (unit + integration)

> Requires Docker running with `docker-compose up -d`

```bash
mvn verify -pl notification-consumer -am
```

**Test coverage:**
- `AdminServiceTest` — 4 unit tests (retry logic, DLQ scenarios)
- `NotificationRouterServiceTest` — 12 unit tests (channel routing per event type)
- `IdempotencyServiceTest` — 4 unit tests (duplicate event detection)
- `NotificationFlowIT` — 5 integration tests (end-to-end Kafka → DB flow)

---

## Key Design Decisions

### 1. Channel Adapter Pattern
New notification channels (WhatsApp, Slack, etc.) are added by creating a single `@Component` implementing `ChannelAdapter`. Zero changes to existing code.

### 2. Idempotency via `notification_events` table
Each Kafka message carries a unique `eventId`. Before processing, the consumer checks if it was already handled — preventing duplicate notifications on consumer restarts or rebalances.

### 3. DLQ with manual retry
Failed events after 3 retries are published to `ml.notifications.dlq`. The Admin API allows inspecting these failures and re-dispatching them through the original channel — without re-publishing to Kafka.

### 4. Manual Kafka ACK
`AckMode.MANUAL_IMMEDIATE` ensures offsets are only committed after successful DB persistence. If the service crashes mid-processing, the event is redelivered.

---

## Author

**Stanley** — [devopstanley@gmail.com](mailto:devopstanley@gmail.com)
