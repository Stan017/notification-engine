package com.ml.notification.consumer.admin.dto;

import lombok.Builder;
import lombok.Data;

import java.util.UUID;

@Data
@Builder
public class RetryResponse {
    private UUID notificationId;
    private String result;   // SUCCESS | FAILED
    private String message;
}
