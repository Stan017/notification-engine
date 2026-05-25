package com.ml.notification.common.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.ml.notification.common.enums.EventType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DomainEvent {

    @Builder.Default
    private String eventId = UUID.randomUUID().toString();

    @NotNull(message = "eventType is required")
    private EventType eventType;

    @NotBlank(message = "userId is required")
    private String userId;

    @Builder.Default
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime timestamp = LocalDateTime.now();

    private Map<String, Object> payload;
}
