package com.ml.notification.consumer.listener;

import com.ml.notification.common.dto.DomainEvent;
import com.ml.notification.common.enums.Channel;
import com.ml.notification.consumer.channel.ChannelAdapter;
import com.ml.notification.consumer.service.IdempotencyService;
import com.ml.notification.consumer.service.NotificationRouterService;
import com.ml.notification.consumer.service.NotificationStatusService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
public class DomainEventListener {

    private final IdempotencyService idempotencyService;
    private final NotificationRouterService routerService;
    private final NotificationStatusService statusService;
    private final Map<Channel, ChannelAdapter> channelAdapters;

    public DomainEventListener(IdempotencyService idempotencyService,
                                NotificationRouterService routerService,
                                NotificationStatusService statusService,
                                List<ChannelAdapter> adapters) {
        this.idempotencyService = idempotencyService;
        this.routerService = routerService;
        this.statusService = statusService;
        this.channelAdapters = adapters.stream()
                .collect(Collectors.toMap(ChannelAdapter::getChannel, Function.identity()));
    }

    @KafkaListener(
        topics = {
            "${kafka.topics.orders}",
            "${kafka.topics.payments}",
            "${kafka.topics.shipments}"
        },
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void onEvent(DomainEvent event, Acknowledgment ack) {
        log.info("Received event {} of type {}", event.getEventId(), event.getEventType());

        if (idempotencyService.isAlreadyProcessed(event.getEventId())) {
            log.warn("Duplicate event {} — skipping", event.getEventId());
            ack.acknowledge();
            return;
        }

        String content = routerService.buildContent(event.getEventType());
        List<Channel> channels = routerService.resolveChannels(event.getEventType());

        for (Channel channel : channels) {
            UUID notifId = statusService.createPending(event, channel, content);
            try {
                channelAdapters.get(channel).send(event, content);
                statusService.markSent(notifId);
            } catch (Exception e) {
                log.error("Channel {} failed for event {}: {}", channel, event.getEventId(), e.getMessage());
                statusService.markFailed(notifId, e.getMessage());
            }
        }

        idempotencyService.markAsProcessed(event);
        ack.acknowledge();
        log.info("Event {} processed via {} channel(s)", event.getEventId(), channels.size());
    }
}
