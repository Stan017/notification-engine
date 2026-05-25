# Distributed Notification Engine — Mercado Libre Portfolio Project
## Planificación Completa

---

## 1. RESUMEN DEL PROYECTO

Sistema de notificaciones distribuido orientado a eventos.
Simula el core de cómo una plataforma como Mercado Libre notifica a sus usuarios
sobre eventos de dominio: órdenes, pagos, envíos.

**Lo que demuestra al reclutador:**
- Arquitectura event-driven real con Kafka
- Microservicios con responsabilidades claras
- Manejo de fallos: reintentos + Dead Letter Queue
- Testing serio: unitario + integración con contenedores reales
- CI/CD listo para producción

---

## 2. ECOSISTEMA SPRING — MAPA MENTAL

```
Spring Boot              → El motor base. Autoconfigura todo.
├── Spring Web           → REST Controllers (@RestController, @GetMapping, etc.)
├── Spring Data JPA      → ORM. Hablas con la BD sin escribir SQL a mano.
│   └── Hibernate        → La implementación JPA que usa Spring por debajo.
├── Spring Kafka         → Productor y consumidor Kafka con anotaciones.
├── Spring Retry         → @Retryable — reintenta automáticamente si falla.
├── Spring Validation    → @Valid, @NotNull, validaciones en DTOs.
└── Spring Test          → Contexto de pruebas + Testcontainers integration.

Herramientas de construcción:
Maven                    → Gestiona dependencias (pom.xml) y empaqueta el JAR/WAR.
```

**Regla de oro de Spring Boot:**
> Si existe un `spring-boot-starter-*`, úsalo. Ya tiene todo configurado.
> Tú solo pones properties en `application.yml`.

---

## 3. ARQUITECTURA

### Estilo: Event-Driven Microservices

```
┌─────────────────────────────────────────────────────────────┐
│                    EXTERNAL SYSTEMS (mock)                   │
│         Orders API │ Payments API │ Shipments API            │
└────────────┬────────────────┬──────────────┬────────────────┘
             │                │              │
             ▼                ▼              ▼
┌─────────────────────────────────────────────────────────────┐
│               notification-producer (REST API)               │
│  POST /events/order  │  /events/payment  │  /events/shipment │
└─────────────────────────────┬───────────────────────────────┘
                              │  publica evento a Kafka
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                        KAFKA BROKER                          │
│  Topics:                                                      │
│  ├── ml.orders.events                                         │
│  ├── ml.payments.events                                       │
│  ├── ml.shipments.events                                      │
│  └── ml.notifications.dlq   (Dead Letter Queue)              │
└─────────────────────────────┬───────────────────────────────┘
                              │  consume eventos
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              notification-consumer (Core Service)            │
│                                                              │
│  EventListener → Router → ChannelAdapter → StatusUpdater    │
│                                                              │
│  Canales disponibles:                                         │
│  ├── EmailChannel   (mock — log a consola)                   │
│  ├── PushChannel    (mock — log a consola)                   │
│  └── SMSChannel     (mock — log a consola)                   │
└──────────────────────────┬──────────────────────────────────┘
                           │  persiste estado
                           ▼
                ┌──────────────────────┐
                │     PostgreSQL        │
                │  notifications        │
                │  notification_events  │
                │  user_preferences     │
                └──────────────────────┘
```

### Módulos del proyecto (estructura Maven multi-módulo)

```
JavaMercadoLibre/
├── pom.xml                          ← parent POM
├── notification-producer/           ← REST API que publica eventos
│   └── src/
├── notification-consumer/           ← Kafka consumer + lógica de negocio
│   └── src/
├── notification-common/             ← DTOs, eventos, constantes compartidos
│   └── src/
└── PLANNING.md
```

---

## 4. EVENTOS DE DOMINIO

Cada evento tiene esta estructura base:

```json
{
  "eventId": "uuid",
  "eventType": "ORDER_SHIPPED",
  "userId": "usr_123",
  "timestamp": "2026-05-24T19:00:00Z",
  "payload": { ... }
}
```

### Tipos de evento

| EventType          | Trigger                        | Canal default |
|--------------------|--------------------------------|---------------|
| ORDER_CREATED      | Usuario hace una compra        | Email + Push  |
| ORDER_SHIPPED      | Vendedor despacha              | Push + SMS    |
| ORDER_DELIVERED    | Confirmación de entrega        | Email + Push  |
| PAYMENT_APPROVED   | Pago procesado OK              | Email         |
| PAYMENT_REJECTED   | Pago rechazado                 | Email + Push  |
| SHIPMENT_UPDATED   | Actualización de tracking      | Push          |

---

## 5. MODELO DE BASE DE DATOS

```sql
-- Registro de cada notificación generada
CREATE TABLE notifications (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         VARCHAR(50)  NOT NULL,
    event_type      VARCHAR(50)  NOT NULL,
    channel         VARCHAR(20)  NOT NULL,  -- EMAIL, PUSH, SMS
    status          VARCHAR(20)  NOT NULL,  -- PENDING, SENT, FAILED, RETRYING
    content         TEXT,
    retry_count     INT          DEFAULT 0,
    created_at      TIMESTAMP    DEFAULT NOW(),
    sent_at         TIMESTAMP,
    error_message   TEXT
);

-- Log crudo de eventos Kafka recibidos
CREATE TABLE notification_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        VARCHAR(100) NOT NULL UNIQUE,
    event_type      VARCHAR(50)  NOT NULL,
    raw_payload     JSONB        NOT NULL,
    received_at     TIMESTAMP    DEFAULT NOW(),
    processed       BOOLEAN      DEFAULT FALSE
);

-- Preferencias de canal por usuario
CREATE TABLE user_preferences (
    user_id         VARCHAR(50)  PRIMARY KEY,
    email_enabled   BOOLEAN      DEFAULT TRUE,
    push_enabled    BOOLEAN      DEFAULT TRUE,
    sms_enabled     BOOLEAN      DEFAULT FALSE
);
```

---

## 6. DIAGRAMA DE FLUJO — HAPPY PATH

```
[POST /events/order]
       │
       ▼
 Validar payload (@Valid)
       │
       ▼
 Asignar eventId (UUID) + timestamp
       │
       ▼
 Publicar a Kafka topic (ml.orders.events)
       │
       ▼
 Return 202 Accepted
       │
       ═══════════════════════════════ (async desde aquí)
       │
       ▼
 KafkaListener recibe evento
       │
       ▼
 Idempotency check: ¿ya procesé este eventId?
  ├── SI → skip, log, ack
  └── NO → continuar
       │
       ▼
 Guardar raw event en notification_events
       │
       ▼
 Consultar user_preferences
       │
       ▼
 Router: determina canales según eventType + preferencias
       │
       ├──── EmailChannel.send()
       ├──── PushChannel.send()
       └──── SMSChannel.send()
              │
              ▼
        ¿Éxito?
        ├── SI → UPDATE notifications SET status='SENT', sent_at=NOW()
        └── NO → retry (max 3)
                  │
                  └── ¿Agotó reintentos?
                       └── SI → publicar a ml.notifications.dlq
                                UPDATE status='FAILED'
```

---

## 7. DIAGRAMA DE FLUJO — DEAD LETTER QUEUE

```
[ml.notifications.dlq consumer]
       │
       ▼
 Recibe mensaje fallido
       │
       ▼
 Log de alerta (simula alerta a equipo de ops)
       │
       ▼
 UPDATE notifications SET status='FAILED', error_message=...
       │
       ▼
 Endpoint admin disponible:
 GET /admin/dlq/notifications  → lista fallidos
 POST /admin/dlq/{id}/retry    → reintento manual
```

---

## 8. ESTRATEGIA DE TESTING

### Pirámide de tests

```
         ▲
        /△\      E2E (1-2 tests — flujo completo)
       /   \
      /─────\    Integration Tests (Testcontainers)
     /       \   — Kafka real + PostgreSQL real en Docker
    /─────────\
   /           \  Unit Tests (mayoría)
  /─────────────\ — Services, Router, Validators con Mockito
 ▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔
```

### Unit Tests (JUnit 5 + Mockito)
- `NotificationRouterTest` — verifica que cada EventType mapea a los canales correctos
- `IdempotencyServiceTest` — verifica que eventos duplicados se ignoran
- `EventPublisherTest` — verifica que el producer llama a KafkaTemplate
- `ChannelAdapterTest` — verifica comportamiento de cada canal (mock del sender)

### Integration Tests (Testcontainers)
- `NotificationFlowIT` — publica evento → verifica que llega al consumer → verifica BD
- `DLQFlowIT` — simula fallo de canal → verifica que llega a DLQ → verifica status
- `RetryFlowIT` — simula fallo transitorio → verifica reintentos → verifica éxito final

### ¿Por qué Testcontainers?
> Levanta Kafka y PostgreSQL reales en Docker durante el test.
> No mocks de infraestructura. Si falla en test, falla igual en prod.

---

## 9. STACK TECNOLÓGICO COMPLETO

| Categoría          | Tecnología                    | Versión sugerida |
|--------------------|-------------------------------|------------------|
| Lenguaje           | Java                          | 21 (LTS)         |
| Framework          | Spring Boot                   | 3.3.x            |
| Messaging          | Apache Kafka                  | 3.7.x            |
| Persistencia       | Spring Data JPA + Hibernate   | incluido en Boot  |
| Base de datos      | PostgreSQL                    | 16               |
| Build tool         | Maven                         | 3.9.x            |
| Testing unitario   | JUnit 5 + Mockito             | incluido en Boot  |
| Testing integración| Testcontainers                | 1.19.x           |
| Contenedores dev   | Docker + Docker Compose       | latest           |
| Documentación API  | SpringDoc OpenAPI (Swagger)   | 2.x              |
| Reintentos         | Spring Retry                  | incluido en Boot  |
| Lombok             | Lombok                        | incluido en Boot  |

---

## 10. PLAN DE ESCALADO

### Nivel 1 — Lo que construimos ahora (1 instancia)
```
1 producer → Kafka → 1 consumer → PostgreSQL
```

### Nivel 2 — Escala horizontal (sin cambiar código)
```
Kafka topic con 3 particiones
→ 3 instancias del consumer en el mismo consumer group
→ Kafka distribuye los mensajes automáticamente
→ PostgreSQL sigue siendo el mismo (connection pool con HikariCP)
```

**Esto funciona porque:**
- El consumer es stateless
- La idempotency check en BD previene procesamiento doble
- Kafka garantiza que cada partición es leída por un solo consumer del grupo

### Nivel 3 — Escala de BD (futuro)
```
PostgreSQL → Read replicas para queries de estado
           → Particionado de tabla notifications por fecha
```

### Nivel 4 — Canales reales (futuro)
```
EmailChannel  → integrar SendGrid / AWS SES
PushChannel   → integrar Firebase FCM
SMSChannel    → integrar Twilio
```

---

## 11. ROADMAP DE IMPLEMENTACIÓN

```
Fase 1 — Infraestructura base         (2-3 días)
  ├── Setup Maven multi-módulo
  ├── Docker Compose (Kafka + PostgreSQL + Zookeeper)
  ├── notification-common: DTOs + eventos
  └── Configuración Spring Boot base

Fase 2 — Producer                     (1-2 días)
  ├── REST Controllers
  ├── Kafka Producer config
  ├── Validaciones (@Valid)
  └── Unit tests del producer

Fase 3 — Consumer + lógica core       (3-4 días)
  ├── Kafka Consumer config
  ├── Router de canales
  ├── Idempotency service
  ├── Channel adapters (mock)
  ├── Retry + DLQ
  └── Unit tests de todo lo anterior

Fase 4 — Persistencia                 (1-2 días)
  ├── Entidades JPA
  ├── Repositories
  ├── Migrations con Flyway
  └── Integration tests con Testcontainers

Fase 5 — Admin API + Swagger          (1 día)
  ├── GET /admin/notifications
  ├── GET /admin/dlq/notifications
  ├── POST /admin/dlq/{id}/retry
  └── Swagger UI

Fase 6 — CI/CD + documentación        (1 día)
  ├── GitHub Actions pipeline
  ├── README con arquitectura
  └── Postman collection
```

---

## 12. ESTRUCTURA DE CARPETAS FINAL

```
JavaMercadoLibre/
├── pom.xml                              ← parent POM (gestiona versiones)
├── docker-compose.yml                   ← Kafka + Zookeeper + PostgreSQL
├── PLANNING.md
├── README.md
│
├── notification-common/
│   └── src/main/java/com/ml/notification/common/
│       ├── dto/
│       │   ├── DomainEvent.java
│       │   └── NotificationResponse.java
│       └── enums/
│           ├── EventType.java
│           ├── Channel.java
│           └── NotificationStatus.java
│
├── notification-producer/
│   └── src/
│       ├── main/java/com/ml/notification/producer/
│       │   ├── ProducerApplication.java
│       │   ├── controller/EventController.java
│       │   ├── service/EventPublisherService.java
│       │   └── config/KafkaProducerConfig.java
│       └── test/
│           └── service/EventPublisherServiceTest.java
│
└── notification-consumer/
    └── src/
        ├── main/java/com/ml/notification/consumer/
        │   ├── ConsumerApplication.java
        │   ├── listener/DomainEventListener.java
        │   ├── service/
        │   │   ├── NotificationRouterService.java
        │   │   ├── IdempotencyService.java
        │   │   └── NotificationStatusService.java
        │   ├── channel/
        │   │   ├── EmailChannelAdapter.java
        │   │   ├── PushChannelAdapter.java
        │   │   └── SMSChannelAdapter.java
        │   ├── entity/
        │   │   ├── Notification.java
        │   │   └── NotificationEvent.java
        │   ├── repository/
        │   │   ├── NotificationRepository.java
        │   │   └── NotificationEventRepository.java
        │   └── config/
        │       └── KafkaConsumerConfig.java
        └── test/
            ├── service/NotificationRouterServiceTest.java
            ├── service/IdempotencyServiceTest.java
            └── integration/NotificationFlowIT.java
```
