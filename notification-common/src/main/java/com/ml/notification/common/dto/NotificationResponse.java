package com.ml.notification.common.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class NotificationResponse {
    private String eventId;
    private String status;
    private String message;
}
