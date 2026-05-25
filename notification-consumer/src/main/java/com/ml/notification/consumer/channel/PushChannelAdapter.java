package com.ml.notification.consumer.channel;

import com.ml.notification.common.dto.DomainEvent;
import com.ml.notification.common.enums.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PushChannelAdapter implements ChannelAdapter {

    @Override
    public void send(DomainEvent event, String content) {
        log.info("[PUSH] → user={} | message='{}' | eventId={}",
                event.getUserId(), content, event.getEventId());
        // Production: integrate Firebase FCM here
    }

    @Override
    public Channel getChannel() {
        return Channel.PUSH;
    }
}
