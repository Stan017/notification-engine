package com.ml.notification.consumer.channel;

import com.ml.notification.common.dto.DomainEvent;
import com.ml.notification.common.enums.Channel;

public interface ChannelAdapter {
    void send(DomainEvent event, String content);
    Channel getChannel();
}
