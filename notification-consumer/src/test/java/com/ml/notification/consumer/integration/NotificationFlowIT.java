package com.ml.notification.consumer.integration;

import com.ml.notification.common.dto.DomainEvent;
import com.ml.notification.common.enums.EventType;
import com.ml.notification.common.enums.NotificationStatus;
import com.ml.notification.consumer.entity.Notification;
import com.ml.notification.consumer.repository.NotificationEventRepository;
import com.ml.notification.consumer.repository.NotificationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Import(AbstractIntegrationTest.TestProducerConfig.class)
@DisplayName("Integration: Notification Flow")
class NotificationFlowIT extends AbstractIntegrationTest {

    @Autowired
    private KafkaTemplate<String, DomainEvent> testKafkaTemplate;

    @Autowired
    private NotificationRepository notificationRepository;

    @Autowired
    private NotificationEventRepository notificationEventRepository;

    @Value("${kafka.topics.orders}")
    private String ordersTopic;

    @Value("${kafka.topics.payments}")
    private String paymentsTopic;

    @BeforeEach
    void cleanUp() {
        notificationRepository.deleteAll();
        notificationEventRepository.deleteAll();
    }

    @Test
    @DisplayName("ORDER_CREATED → generates EMAIL and PUSH notifications saved as SENT")
    void orderCreated_generatesTwoNotificationsAsSent() {
        DomainEvent event = DomainEvent.builder()
                .eventType(EventType.ORDER_CREATED)
                .userId("usr_order_test")
                .build();

        testKafkaTemplate.send(ordersTopic, event.getEventId(), event);

        await()
            .atMost(15, TimeUnit.SECONDS)
            .until(() -> notificationRepository.findByUserId("usr_order_test").size() >= 2);

        List<Notification> notifications = notificationRepository.findByUserId("usr_order_test");

        assertThat(notifications).hasSize(2);
        assertThat(notifications)
                .extracting(Notification::getStatus)
                .containsOnly(NotificationStatus.SENT);
        assertThat(notifications)
                .extracting(n -> n.getChannel().name())
                .containsExactlyInAnyOrder("EMAIL", "PUSH");
    }

    @Test
    @DisplayName("ORDER_SHIPPED → generates PUSH and SMS notifications")
    void orderShipped_generatesPushAndSms() {
        DomainEvent event = DomainEvent.builder()
                .eventType(EventType.ORDER_SHIPPED)
                .userId("usr_shipped_test")
                .build();

        testKafkaTemplate.send(ordersTopic, event.getEventId(), event);

        await()
            .atMost(15, TimeUnit.SECONDS)
            .until(() -> notificationRepository.findByUserId("usr_shipped_test").size() >= 2);

        List<Notification> notifications = notificationRepository.findByUserId("usr_shipped_test");

        assertThat(notifications)
                .extracting(n -> n.getChannel().name())
                .containsExactlyInAnyOrder("PUSH", "SMS");
    }

    @Test
    @DisplayName("PAYMENT_APPROVED → generates only EMAIL notification")
    void paymentApproved_generatesOnlyEmail() {
        DomainEvent event = DomainEvent.builder()
                .eventType(EventType.PAYMENT_APPROVED)
                .userId("usr_payment_test")
                .build();

        testKafkaTemplate.send(paymentsTopic, event.getEventId(), event);

        await()
            .atMost(15, TimeUnit.SECONDS)
            .until(() -> notificationRepository.findByUserId("usr_payment_test").size() >= 1);

        List<Notification> notifications = notificationRepository.findByUserId("usr_payment_test");

        assertThat(notifications).hasSize(1);
        assertThat(notifications.get(0).getChannel().name()).isEqualTo("EMAIL");
    }

    @Test
    @DisplayName("Duplicate event is processed only once (idempotency)")
    void duplicateEvent_isIgnored() throws InterruptedException {
        DomainEvent event = DomainEvent.builder()
                .eventType(EventType.PAYMENT_APPROVED)
                .userId("usr_dup_test")
                .build();

        // Publicar el mismo evento dos veces
        testKafkaTemplate.send(paymentsTopic, event.getEventId(), event);
        testKafkaTemplate.send(paymentsTopic, event.getEventId(), event);

        // Esperar a que el primer evento sea procesado
        await()
            .atMost(15, TimeUnit.SECONDS)
            .until(() -> notificationEventRepository.existsByEventId(event.getEventId()));

        // Dar tiempo para que el duplicado también se intente procesar
        TimeUnit.SECONDS.sleep(3);

        // Solo debe existir 1 notificación (PAYMENT_APPROVED → EMAIL)
        List<Notification> notifications = notificationRepository.findByUserId("usr_dup_test");
        assertThat(notifications).hasSize(1);
    }

    @Test
    @DisplayName("Processed event is recorded in notification_events")
    void processedEvent_isRecordedInEventLog() {
        DomainEvent event = DomainEvent.builder()
                .eventType(EventType.ORDER_DELIVERED)
                .userId("usr_log_test")
                .build();

        testKafkaTemplate.send(ordersTopic, event.getEventId(), event);

        await()
            .atMost(15, TimeUnit.SECONDS)
            .until(() -> notificationEventRepository.existsByEventId(event.getEventId()));

        assertThat(notificationEventRepository.existsByEventId(event.getEventId())).isTrue();
    }
}
