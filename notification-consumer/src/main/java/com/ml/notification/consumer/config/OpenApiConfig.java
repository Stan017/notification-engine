package com.ml.notification.consumer.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI notificationEngineOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Distributed Notification Engine — Consumer")
                        .version("1.0.0")
                        .description("""
                                Event-driven notification system built with Spring Boot + Kafka.

                                **Flujo:**
                                1. El producer publica un `DomainEvent` a Kafka
                                2. El consumer lo procesa y notifica al usuario via EMAIL / PUSH / SMS
                                3. Si falla, el evento va al Dead Letter Queue (DLQ)
                                4. La Admin API permite inspeccionar y reintentar notificaciones fallidas
                                """)
                        .contact(new Contact()
                                .name("Stanley")
                                .email("devopstanley@gmail.com")))
                .servers(List.of(
                        new Server().url("http://localhost:8081").description("Local")));
    }
}
