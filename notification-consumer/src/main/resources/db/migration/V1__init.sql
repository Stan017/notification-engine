-- Registro de cada notificación generada
CREATE TABLE notifications (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       VARCHAR(50)  NOT NULL,
    event_type    VARCHAR(50)  NOT NULL,
    channel       VARCHAR(20)  NOT NULL,
    status        VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
    content       TEXT,
    retry_count   INT          NOT NULL DEFAULT 0,
    created_at    TIMESTAMP    NOT NULL DEFAULT NOW(),
    sent_at       TIMESTAMP,
    error_message TEXT
);

-- Log crudo de eventos Kafka recibidos (para idempotency check)
CREATE TABLE notification_events (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id    VARCHAR(100) NOT NULL UNIQUE,
    event_type  VARCHAR(50)  NOT NULL,
    raw_payload JSONB        NOT NULL,
    received_at TIMESTAMP    NOT NULL DEFAULT NOW(),
    processed   BOOLEAN      NOT NULL DEFAULT FALSE
);

-- Preferencias de canal por usuario
CREATE TABLE user_preferences (
    user_id       VARCHAR(50) PRIMARY KEY,
    email_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    push_enabled  BOOLEAN NOT NULL DEFAULT TRUE,
    sms_enabled   BOOLEAN NOT NULL DEFAULT FALSE
);

-- Índices para queries frecuentes
CREATE INDEX idx_notifications_user_id    ON notifications(user_id);
CREATE INDEX idx_notifications_status     ON notifications(status);
CREATE INDEX idx_notifications_event_type ON notifications(event_type);
CREATE INDEX idx_notification_events_processed ON notification_events(processed);
