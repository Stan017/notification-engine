package com.ml.notification.consumer.service;

import com.ml.notification.common.enums.Channel;
import com.ml.notification.common.enums.EventType;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class NotificationRouterService {

    public List<Channel> resolveChannels(EventType eventType) {
        return switch (eventType) {
            case ORDER_CREATED    -> List.of(Channel.EMAIL, Channel.PUSH);
            case ORDER_SHIPPED    -> List.of(Channel.PUSH, Channel.SMS);
            case ORDER_DELIVERED  -> List.of(Channel.EMAIL, Channel.PUSH);
            case PAYMENT_APPROVED -> List.of(Channel.EMAIL);
            case PAYMENT_REJECTED -> List.of(Channel.EMAIL, Channel.PUSH);
            case SHIPMENT_UPDATED -> List.of(Channel.PUSH);
        };
    }

    public String buildContent(EventType eventType) {
        return switch (eventType) {
            case ORDER_CREATED    -> "Your order has been confirmed!";
            case ORDER_SHIPPED    -> "Your order is on its way!";
            case ORDER_DELIVERED  -> "Your order has been delivered. Enjoy!";
            case PAYMENT_APPROVED -> "Payment confirmed successfully.";
            case PAYMENT_REJECTED -> "We could not process your payment. Please try again.";
            case SHIPMENT_UPDATED -> "There is a new update on your shipment.";
        };
    }
}
