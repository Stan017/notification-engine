package com.ml.notification.consumer.repository;

import com.ml.notification.consumer.entity.Notification;
import com.ml.notification.common.enums.NotificationStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface NotificationRepository extends JpaRepository<Notification, UUID> {
    List<Notification> findByStatus(NotificationStatus status);
    List<Notification> findByUserId(String userId);
}
