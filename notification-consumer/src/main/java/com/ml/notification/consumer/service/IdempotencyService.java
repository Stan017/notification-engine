package com.ml.notification.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ml.notification.common.dto.DomainEvent;
import com.ml.notification.consumer.entity.NotificationEvent;
import com.ml.notification.consumer.repository.NotificationEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final NotificationEventRepository eventRepository;
    private final ObjectMapper objectMapper;

    public boolean isAlreadyProcessed(String eventId) {
        return eventRepository.existsByEventId(eventId);
    }

    @Transactional
    public void markAsProcessed(DomainEvent event) {
        NotificationEvent record = NotificationEvent.builder()
                .eventId(event.getEventId())
                .eventType(event.getEventType())
                .rawPayload(serialize(event))
                .processed(true)
                .build();
        eventRepository.save(record);
    }

    private String serialize(DomainEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            log.warn("Could not serialize event {}: {}", event.getEventId(), e.getMessage());
            return "{}";
        }
    }
}
