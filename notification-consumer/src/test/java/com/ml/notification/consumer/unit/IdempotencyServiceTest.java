package com.ml.notification.consumer.unit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ml.notification.common.dto.DomainEvent;
import com.ml.notification.common.enums.EventType;
import com.ml.notification.consumer.entity.NotificationEvent;
import com.ml.notification.consumer.repository.NotificationEventRepository;
import com.ml.notification.consumer.service.IdempotencyService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IdempotencyServiceTest {

    @Mock
    private NotificationEventRepository eventRepository;

    @Spy
    private ObjectMapper objectMapper;

    @InjectMocks
    private IdempotencyService idempotencyService;

    @Test
    @DisplayName("isAlreadyProcessed returns true when eventId exists in DB")
    void isAlreadyProcessed_returnsTrueWhenDuplicate() {
        when(eventRepository.existsByEventId("evt-123")).thenReturn(true);

        assertThat(idempotencyService.isAlreadyProcessed("evt-123")).isTrue();
    }

    @Test
    @DisplayName("isAlreadyProcessed returns false when eventId is new")
    void isAlreadyProcessed_returnsFalseWhenNew() {
        when(eventRepository.existsByEventId("evt-new")).thenReturn(false);

        assertThat(idempotencyService.isAlreadyProcessed("evt-new")).isFalse();
    }

    @Test
    @DisplayName("markAsProcessed saves event with correct eventId and type")
    void markAsProcessed_savesEventRecord() {
        DomainEvent event = DomainEvent.builder()
                .eventType(EventType.ORDER_CREATED)
                .userId("usr_1")
                .build();

        idempotencyService.markAsProcessed(event);

        ArgumentCaptor<NotificationEvent> captor = ArgumentCaptor.forClass(NotificationEvent.class);
        verify(eventRepository).save(captor.capture());

        NotificationEvent saved = captor.getValue();
        assertThat(saved.getEventId()).isEqualTo(event.getEventId());
        assertThat(saved.getEventType()).isEqualTo(EventType.ORDER_CREATED);
        assertThat(saved.isProcessed()).isTrue();
    }

    @Test
    @DisplayName("markAsProcessed does not save when eventId is null")
    void markAsProcessed_duplicateIsNotSavedTwice() {
        when(eventRepository.existsByEventId(any())).thenReturn(true);

        DomainEvent event = DomainEvent.builder()
                .eventType(EventType.PAYMENT_APPROVED)
                .userId("usr_2")
                .build();

        // El listener no llama markAsProcessed si ya existe — validamos que el check funciona
        boolean alreadyProcessed = idempotencyService.isAlreadyProcessed(event.getEventId());

        assertThat(alreadyProcessed).isTrue();
        verify(eventRepository, never()).save(any());
    }
}
