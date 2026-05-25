package com.ml.notification.producer.service;

import com.ml.notification.common.dto.DomainEvent;
import com.ml.notification.common.dto.NotificationResponse;
import com.ml.notification.common.enums.EventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EventPublisherServiceTest {

    @Mock
    private KafkaTemplate<String, DomainEvent> kafkaTemplate;

    @InjectMocks
    private EventPublisherService publisherService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(publisherService, "ordersTopic",   "ml.orders.events");
        ReflectionTestUtils.setField(publisherService, "paymentsTopic", "ml.payments.events");
        ReflectionTestUtils.setField(publisherService, "shipmentsTopic","ml.shipments.events");

        when(kafkaTemplate.send(anyString(), anyString(), any(DomainEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
    }

    @Test
    @DisplayName("publish() returns ACCEPTED response with the same eventId")
    void publish_returnsAcceptedResponse() {
        DomainEvent event = DomainEvent.builder()
                .eventType(EventType.ORDER_CREATED)
                .userId("usr_123")
                .build();

        NotificationResponse response = publisherService.publish(event);

        assertThat(response.getStatus()).isEqualTo("ACCEPTED");
        assertThat(response.getEventId()).isEqualTo(event.getEventId());
    }

    @ParameterizedTest
    @EnumSource(value = EventType.class, names = {"ORDER_CREATED", "ORDER_SHIPPED", "ORDER_DELIVERED"})
    @DisplayName("Order events are routed to ml.orders.events topic")
    void publish_routesOrderEventsToOrdersTopic(EventType eventType) {
        DomainEvent event = DomainEvent.builder().eventType(eventType).userId("usr_1").build();

        publisherService.publish(event);

        verify(kafkaTemplate).send(eq("ml.orders.events"), eq(event.getEventId()), eq(event));
    }

    @ParameterizedTest
    @EnumSource(value = EventType.class, names = {"PAYMENT_APPROVED", "PAYMENT_REJECTED"})
    @DisplayName("Payment events are routed to ml.payments.events topic")
    void publish_routesPaymentEventsToPaymentsTopic(EventType eventType) {
        DomainEvent event = DomainEvent.builder().eventType(eventType).userId("usr_1").build();

        publisherService.publish(event);

        verify(kafkaTemplate).send(eq("ml.payments.events"), eq(event.getEventId()), eq(event));
    }

    @Test
    @DisplayName("Shipment events are routed to ml.shipments.events topic")
    void publish_routesShipmentEventsToShipmentsTopic() {
        DomainEvent event = DomainEvent.builder().eventType(EventType.SHIPMENT_UPDATED).userId("usr_1").build();

        publisherService.publish(event);

        verify(kafkaTemplate).send(eq("ml.shipments.events"), eq(event.getEventId()), eq(event));
    }
}
