package com.ml.notification.consumer.repository;

import com.ml.notification.consumer.entity.NotificationEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface NotificationEventRepository extends JpaRepository<NotificationEvent, UUID> {
    boolean existsByEventId(String eventId);
}
