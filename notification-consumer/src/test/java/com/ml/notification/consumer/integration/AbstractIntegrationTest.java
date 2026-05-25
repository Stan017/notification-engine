package com.ml.notification.consumer.integration;

import com.ml.notification.common.dto.DomainEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.ActiveProfiles;

import java.util.HashMap;
import java.util.Map;

/**
 * Base class for integration tests.
 *
 * Prerequisites (run once before tests):
 *   docker-compose up -d
 *
 * Teardown:
 *   docker-compose down
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
abstract class AbstractIntegrationTest {

    // KafkaTemplate para publicar eventos desde los tests (simula el producer service)
    @TestConfiguration
    static class TestProducerConfig {

        @Bean
        @Primary
        public KafkaTemplate<String, DomainEvent> testKafkaTemplate(
                @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
            Map<String, Object> config = new HashMap<>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
            return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
        }
    }
}
