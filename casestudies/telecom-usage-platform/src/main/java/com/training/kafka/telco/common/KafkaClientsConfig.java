package com.training.kafka.telco.common;

import java.util.Map;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Two producers with two different durability contracts (Module 3 sections 6.5 and 7).
 *
 * "kafkaTemplate"      acks=all, idempotence, zstd     business data: CDRs, plans, charges, audit
 * "telemetryTemplate"  acks=1, no idempotence, lz4     telemetry: speed and cost over certainty
 *
 * The durability of a write is decided by BOTH sides: the producer's acks and
 * the topic's min.insync.replicas. The topic design (TopicCatalog) says what the
 * cluster will accept; these templates say what each caller asks for.
 *
 * Declaring our own KafkaTemplate makes Spring Boot's default one back off, so
 * the default has to be declared here too.
 */
@Configuration
public class KafkaClientsConfig {

    @Bean
    @Primary
    KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    @Qualifier("telemetryTemplate")
    KafkaTemplate<String, String> telemetryTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory.copyWithConfigurationOverride(Map.of(
                "acks", "1",
                "enable.idempotence", "false",
                "compression.type", "lz4",
                "linger.ms", "100",
                "batch.size", "65536")));
    }
}
