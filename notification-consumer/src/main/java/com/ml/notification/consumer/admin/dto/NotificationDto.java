package com.ml.notification.consumer.admin.dto;

import com.ml.notification.common.enums.Channel;
import com.ml.notification.common.enums.EventType;
import com.ml.notification.common.enums.NotificationStatus;
import com.ml.notification.consumer.entity.Notification;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
public class NotificationDto {

    private UUID id;
    private String userId;
    private EventType eventType;
    private Channel channel;
    private NotificationStatus status;
    private String content;
    private int retryCount;
    private LocalDateTime createdAt;
    private LocalDateTime sentAt;
    private String errorMessage;

    public static NotificationDto from(Notification n) {
        return NotificationDto.builder()
                .id(n.getId())
                .userId(n.getUserId())
                .eventType(n.getEventType())
                .channel(n.getChannel())
                .status(n.getStatus())
                .content(n.getContent())
                .retryCount(n.getRetryCount())
                .createdAt(n.getCreatedAt())
                .sentAt(n.getSentAt())
                .errorMessage(n.getErrorMessage())
                .build();
    }
}
