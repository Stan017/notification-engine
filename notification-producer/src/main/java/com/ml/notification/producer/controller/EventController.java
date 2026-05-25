package com.ml.notification.producer.controller;

import com.ml.notification.common.dto.DomainEvent;
import com.ml.notification.common.dto.NotificationResponse;
import com.ml.notification.producer.service.EventPublisherService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
@Tag(name = "Events", description = "Publish domain events to the notification pipeline")
public class EventController {

    private final EventPublisherService publisherService;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    @Operation(
        summary = "Publish a domain event",
        description = "Accepts an event and routes it to the correct Kafka topic based on eventType"
    )
    public NotificationResponse publish(@Valid @RequestBody DomainEvent event) {
        return publisherService.publish(event);
    }
}
