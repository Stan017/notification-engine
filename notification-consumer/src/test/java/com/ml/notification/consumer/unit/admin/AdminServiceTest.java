package com.ml.notification.consumer.unit.admin;

import com.ml.notification.common.enums.Channel;
import com.ml.notification.common.enums.EventType;
import com.ml.notification.common.enums.NotificationStatus;
import com.ml.notification.consumer.admin.dto.NotificationDto;
import com.ml.notification.consumer.admin.dto.RetryResponse;
import com.ml.notification.consumer.admin.service.AdminService;
import com.ml.notification.consumer.channel.ChannelAdapter;
import com.ml.notification.consumer.entity.Notification;
import com.ml.notification.consumer.repository.NotificationRepository;
import com.ml.notification.consumer.service.NotificationRouterService;
import com.ml.notification.consumer.service.NotificationStatusService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AdminServiceTest {

    @Mock private NotificationRepository notificationRepository;
    @Mock private NotificationStatusService statusService;
    @Mock private NotificationRouterService routerService;
    @Mock private ChannelAdapter emailAdapter;

    private AdminService adminService;

    @BeforeEach
    void setUp() {
        when(emailAdapter.getChannel()).thenReturn(Channel.EMAIL);
        adminService = new AdminService(
                notificationRepository, statusService, routerService, List.of(emailAdapter));
    }

    @Test
    @DisplayName("getByStatus returns only notifications matching the given status")
    void getByStatus_returnsFilteredNotifications() {
        Notification failed = buildNotification(NotificationStatus.FAILED, Channel.EMAIL);
        when(notificationRepository.findByStatus(NotificationStatus.FAILED)).thenReturn(List.of(failed));

        List<NotificationDto> result = adminService.getByStatus(NotificationStatus.FAILED);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getStatus()).isEqualTo(NotificationStatus.FAILED);
    }

    @Test
    @DisplayName("retry on FAILED notification dispatches channel and returns SUCCESS")
    void retry_onFailedNotification_returnsSuccess() {
        UUID id = UUID.randomUUID();
        Notification failed = buildNotification(id, NotificationStatus.FAILED, Channel.EMAIL);
        when(notificationRepository.findById(id)).thenReturn(Optional.of(failed));
        when(routerService.buildContent(any())).thenReturn("Your order is ready!");

        RetryResponse response = adminService.retry(id);

        assertThat(response.getResult()).isEqualTo("SUCCESS");
        verify(emailAdapter).send(any(), eq("Your order is ready!"));
        verify(statusService).markSent(id);
    }

    @Test
    @DisplayName("retry on non-FAILED notification returns SKIPPED")
    void retry_onSentNotification_returnsSkipped() {
        UUID id = UUID.randomUUID();
        Notification sent = buildNotification(id, NotificationStatus.SENT, Channel.EMAIL);
        when(notificationRepository.findById(id)).thenReturn(Optional.of(sent));

        RetryResponse response = adminService.retry(id);

        assertThat(response.getResult()).isEqualTo("SKIPPED");
        verify(emailAdapter, never()).send(any(), any());
    }

    @Test
    @DisplayName("retry when channel throws returns FAILED response")
    void retry_whenChannelFails_returnsFailed() {
        UUID id = UUID.randomUUID();
        Notification failed = buildNotification(id, NotificationStatus.FAILED, Channel.EMAIL);
        when(notificationRepository.findById(id)).thenReturn(Optional.of(failed));
        when(routerService.buildContent(any())).thenReturn("content");
        doThrow(new RuntimeException("SMTP timeout")).when(emailAdapter).send(any(), any());

        RetryResponse response = adminService.retry(id);

        assertThat(response.getResult()).isEqualTo("FAILED");
        assertThat(response.getMessage()).contains("SMTP timeout");
        verify(statusService).markFailed(eq(id), contains("SMTP timeout"));
    }

    private Notification buildNotification(NotificationStatus status, Channel channel) {
        return buildNotification(UUID.randomUUID(), status, channel);
    }

    private Notification buildNotification(UUID id, NotificationStatus status, Channel channel) {
        return Notification.builder()
                .id(id)
                .userId("usr_test")
                .eventType(EventType.ORDER_CREATED)
                .channel(channel)
                .status(status)
                .content("Test content")
                .build();
    }
}
