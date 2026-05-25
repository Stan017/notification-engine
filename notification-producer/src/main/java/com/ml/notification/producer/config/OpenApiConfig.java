package com.ml.notification.producer.config;

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
    public OpenAPI notificationProducerOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Distributed Notification Engine — Producer")
                        .version("1.0.0")
                        .description("""
                                REST API para publicar eventos de dominio al pipeline de notificaciones.

                                **Eventos soportados:**
                                - `ORDER_CREATED` / `ORDER_SHIPPED` / `ORDER_DELIVERED`
                                - `PAYMENT_APPROVED` / `PAYMENT_REJECTED`
                                - `SHIPMENT_UPDATED`
                                """)
                        .contact(new Contact()
                                .name("Stanley")
                                .email("devopstanley@gmail.com")))
                .servers(List.of(
                        new Server().url("http://localhost:8080").description("Local")));
    }
}
