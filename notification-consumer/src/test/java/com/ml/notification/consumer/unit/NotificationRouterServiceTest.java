package com.ml.notification.consumer.unit;

import com.ml.notification.common.enums.Channel;
import com.ml.notification.common.enums.EventType;
import com.ml.notification.consumer.service.NotificationRouterService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationRouterServiceTest {

    private NotificationRouterService routerService;

    @BeforeEach
    void setUp() {
        routerService = new NotificationRouterService();
    }

    @ParameterizedTest(name = "{0} → channels: {1}")
    @MethodSource("channelRoutingCases")
    @DisplayName("Each EventType resolves to the correct channels")
    void resolveChannels_returnsCorrectChannels(EventType eventType, List<Channel> expectedChannels) {
        List<Channel> result = routerService.resolveChannels(eventType);
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectedChannels);
    }

    @ParameterizedTest(name = "{0} → content not blank")
    @MethodSource("allEventTypes")
    @DisplayName("buildContent returns non-blank message for every EventType")
    void buildContent_neverReturnsBlank(EventType eventType) {
        String content = routerService.buildContent(eventType);
        assertThat(content).isNotBlank();
    }

    static Stream<Arguments> channelRoutingCases() {
        return Stream.of(
            Arguments.of(EventType.ORDER_CREATED,    List.of(Channel.EMAIL, Channel.PUSH)),
            Arguments.of(EventType.ORDER_SHIPPED,    List.of(Channel.PUSH, Channel.SMS)),
            Arguments.of(EventType.ORDER_DELIVERED,  List.of(Channel.EMAIL, Channel.PUSH)),
            Arguments.of(EventType.PAYMENT_APPROVED, List.of(Channel.EMAIL)),
            Arguments.of(EventType.PAYMENT_REJECTED, List.of(Channel.EMAIL, Channel.PUSH)),
            Arguments.of(EventType.SHIPMENT_UPDATED, List.of(Channel.PUSH))
        );
    }

    static Stream<Arguments> allEventTypes() {
        return Stream.of(EventType.values()).map(Arguments::of);
    }
}
