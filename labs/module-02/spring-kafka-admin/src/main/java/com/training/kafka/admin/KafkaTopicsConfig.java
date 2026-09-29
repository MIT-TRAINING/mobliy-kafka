package com.training.kafka.admin;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Topics declared as code.
 *
 * At startup Spring's KafkaAdmin compares every NewTopic bean with the cluster:
 * missing topics are created, a higher partition count is applied, and (with
 * spring.kafka.admin.modify-topic-configs=true) differing topic configs are
 * put back to what is declared here. It never deletes topics or partitions.
 *
 * CLI equivalent of the first bean:
 *   kafka-topics.sh --create --topic cdr.data --partitions 6 --replication-factor 3 \
 *     --config min.insync.replicas=2 --config retention.ms=604800000
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    NewTopic cdrTopic(@Value("${lab.topics.cdr}") String name,
                      @Value("${lab.topics.cdr-partitions}") int partitions,
                      @Value("${lab.topics.cdr-replicas}") int replicas) {
        return TopicBuilder.name(name)
                .partitions(partitions)
                .replicas(replicas)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000") // 7 days
                .build();
    }

    @Bean
    NewTopic subscriberTopic(@Value("${lab.topics.subscriber}") String name) {
        return TopicBuilder.name(name)
                .partitions(3)
                .replicas(3)
                .compact() // cleanup.policy=compact: keep the latest value per key
                .build();
    }

    /**
     * A plain Kafka Admin client for the REST endpoints. It reuses the
     * connection settings Spring Boot built for KafkaAdmin (spring.kafka.*).
     * This is the same Java API that kafka-topics.sh and the other CLI tools
     * call under the hood.
     */
    @Bean(destroyMethod = "close")
    Admin adminClient(KafkaAdmin kafkaAdmin) {
        return Admin.create(kafkaAdmin.getConfigurationProperties());
    }
}
