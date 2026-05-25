package com.ml.notification.consumer.admin.service;

import com.ml.notification.common.dto.DomainEvent;
import com.ml.notification.common.enums.NotificationStatus;
import com.ml.notification.consumer.admin.dto.NotificationDto;
import com.ml.notification.consumer.admin.dto.RetryResponse;
import com.ml.notification.consumer.channel.ChannelAdapter;
import com.ml.notification.consumer.entity.Notification;
import com.ml.notification.consumer.repository.NotificationRepository;
import com.ml.notification.consumer.service.NotificationRouterService;
import com.ml.notification.consumer.service.NotificationStatusService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class AdminService {

    private final NotificationRepository notificationRepository;
    private final NotificationStatusService statusService;
    private final NotificationRouterService routerService;
    private final Map<com.ml.notification.common.enums.Channel, ChannelAdapter> channelAdapters;

    public AdminService(NotificationRepository notificationRepository,
                        NotificationStatusService statusService,
                        NotificationRouterService routerService,
                        List<ChannelAdapter> adapters) {
        this.notificationRepository = notificationRepository;
        this.statusService = statusService;
        this.routerService = routerService;
        this.channelAdapters = adapters.stream()
                .collect(Collectors.toMap(ChannelAdapter::getChannel, Function.identity()));
    }

    public List<NotificationDto> getAll() {
        return notificationRepository.findAll().stream()
                .map(NotificationDto::from)
                .collect(Collectors.toList());
    }

    public List<NotificationDto> getByStatus(NotificationStatus status) {
        return notificationRepository.findByStatus(status).stream()
                .map(NotificationDto::from)
                .collect(Collectors.toList());
    }

    public NotificationDto getById(UUID id) {
        return notificationRepository.findById(id)
                .map(NotificationDto::from)
                .orElseThrow(() -> new RuntimeException("Notification not found: " + id));
    }

    public RetryResponse retry(UUID notificationId) {
        Notification notification = notificationRepository.findById(notificationId)
                .orElseThrow(() -> new RuntimeException("Notification not found: " + notificationId));

        if (notification.getStatus() != NotificationStatus.FAILED) {
            return RetryResponse.builder()
                    .notificationId(notificationId)
                    .result("SKIPPED")
                    .message("Only FAILED notifications can be retried. Current status: " + notification.getStatus())
                    .build();
        }

        // Reconstruir un evento mínimo para el reintento
        DomainEvent event = DomainEvent.builder()
                .userId(notification.getUserId())
                .eventType(notification.getEventType())
                .build();

        String content = routerService.buildContent(notification.getEventType());

        notification.setStatus(NotificationStatus.PENDING);
        notificationRepository.save(notification);

        try {
            channelAdapters.get(notification.getChannel()).send(event, content);
            statusService.markSent(notificationId);
            log.info("Manual retry succeeded for notification {}", notificationId);

            return RetryResponse.builder()
                    .notificationId(notificationId)
                    .result("SUCCESS")
                    .message("Notification re-sent via " + notification.getChannel())
                    .build();
        } catch (Exception e) {
            statusService.markFailed(notificationId, e.getMessage());
            log.error("Manual retry failed for notification {}: {}", notificationId, e.getMessage());

            return RetryResponse.builder()
                    .notificationId(notificationId)
                    .result("FAILED")
                    .message("Retry failed: " + e.getMessage())
                    .build();
        }
    }
}
