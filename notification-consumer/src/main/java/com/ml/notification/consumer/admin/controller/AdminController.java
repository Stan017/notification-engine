package com.ml.notification.consumer.admin.controller;

import com.ml.notification.common.enums.NotificationStatus;
import com.ml.notification.consumer.admin.dto.NotificationDto;
import com.ml.notification.consumer.admin.dto.RetryResponse;
import com.ml.notification.consumer.admin.service.AdminService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/admin")
@RequiredArgsConstructor
@Tag(name = "Admin", description = "Inspect and manage notifications — DLQ monitoring and manual retries")
public class AdminController {

    private final AdminService adminService;

    @GetMapping("/notifications")
    @Operation(
        summary = "List all notifications",
        description = "Returns all notifications. Use `?status=FAILED` to filter by status."
    )
    public List<NotificationDto> getAll(
            @Parameter(description = "Filter by status: PENDING, SENT, FAILED, RETRYING")
            @RequestParam(required = false) NotificationStatus status) {
        if (status != null) {
            return adminService.getByStatus(status);
        }
        return adminService.getAll();
    }

    @GetMapping("/notifications/{id}")
    @Operation(summary = "Get notification by ID")
    public NotificationDto getById(@PathVariable UUID id) {
        return adminService.getById(id);
    }

    @GetMapping("/dlq")
    @Operation(
        summary = "List DLQ — failed notifications",
        description = "Returns all notifications with status FAILED that require manual intervention."
    )
    public List<NotificationDto> getDlq() {
        return adminService.getByStatus(NotificationStatus.FAILED);
    }

    @PostMapping("/dlq/{id}/retry")
    @Operation(
        summary = "Retry a failed notification",
        description = "Re-dispatches a FAILED notification through its original channel. Updates status to SENT on success."
    )
    public RetryResponse retry(@PathVariable UUID id) {
        return adminService.retry(id);
    }
}
