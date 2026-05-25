package com.ml.notification.producer.service;

import com.ml.notification.common.dto.DomainEvent;
import com.ml.notification.common.dto.NotificationResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventPublisherService {

    private final KafkaTemplate<String, DomainEvent> kafkaTemplate;

    @Value("${kafka.topics.orders}")
    private String ordersTopic;

    @Value("${kafka.topics.payments}")
    private String paymentsTopic;

    @Value("${kafka.topics.shipments}")
    private String shipmentsTopic;

    public NotificationResponse publish(DomainEvent event) {
        String topic = resolveTopic(event);

        kafkaTemplate.send(topic, event.getEventId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish event {} to topic {}: {}", event.getEventId(), topic, ex.getMessage());
                    } else {
                        log.info("Event {} published to {}@partition-{}", event.getEventId(), topic,
                                result.getRecordMetadata().partition());
                    }
                });

        return NotificationResponse.builder()
                .eventId(event.getEventId())
                .status("ACCEPTED")
                .message("Event queued for processing")
                .build();
    }

    private String resolveTopic(DomainEvent event) {
        return switch (event.getEventType()) {
            case ORDER_CREATED, ORDER_SHIPPED, ORDER_DELIVERED -> ordersTopic;
            case PAYMENT_APPROVED, PAYMENT_REJECTED            -> paymentsTopic;
            case SHIPMENT_UPDATED                              -> shipmentsTopic;
        };
    }
}
