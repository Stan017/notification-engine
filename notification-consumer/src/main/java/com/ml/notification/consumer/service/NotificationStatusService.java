package com.ml.notification.consumer.service;

import com.ml.notification.common.dto.DomainEvent;
import com.ml.notification.common.enums.Channel;
import com.ml.notification.common.enums.NotificationStatus;
import com.ml.notification.consumer.entity.Notification;
import com.ml.notification.consumer.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class NotificationStatusService {

    private final NotificationRepository notificationRepository;

    @Transactional
    public UUID createPending(DomainEvent event, Channel channel, String content) {
        Notification notification = Notification.builder()
                .userId(event.getUserId())
                .eventType(event.getEventType())
                .channel(channel)
                .status(NotificationStatus.PENDING)
                .content(content)
                .build();
        return notificationRepository.save(notification).getId();
    }

    @Transactional
    public void markSent(UUID notificationId) {
        notificationRepository.findById(notificationId).ifPresent(n -> {
            n.setStatus(NotificationStatus.SENT);
            n.setSentAt(LocalDateTime.now());
        });
    }

    @Transactional
    public void markFailed(UUID notificationId, String errorMessage) {
        notificationRepository.findById(notificationId).ifPresent(n -> {
            n.setStatus(NotificationStatus.FAILED);
            n.setRetryCount(n.getRetryCount() + 1);
            n.setErrorMessage(errorMessage);
        });
    }
}
