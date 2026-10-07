package com.training.kafka.telco.topics;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Topics declared as code (Module 2, Lab 03).
 *
 * At startup KafkaAdmin compares every NewTopic with the cluster: missing topics
 * are created, a higher partition count is applied and, with
 * spring.kafka.admin.modify-topic-configs=true, topic configs that drifted are
 * set back to the catalog values. It never deletes a topic or reduces partitions.
 *
 * CLI equivalent for cdr.voice:
 *   kafka-topics.sh --create --topic cdr.voice --partitions 6 --replication-factor 3 \
 *     --config retention.ms=604800000 --config segment.bytes=268435456 \
 *     --config min.insync.replicas=2 --config cleanup.policy=delete
 */
@Configuration
public class TopicsConfig {

    @Bean
    KafkaAdmin.NewTopics telcoTopics(TopicCatalog catalog, KafkaAdmin kafkaAdmin) {
        if (catalog.confluentCloud()) {
            // Runs before KafkaAdmin creates the topics, and names any setting Cloud refuses
            CloudTopicPreflight.check(kafkaAdmin.getConfigurationProperties(), catalog.specs());
        }
        NewTopic[] topics = catalog.specs().stream()
                .map(spec -> TopicBuilder.name(spec.name())
                        .partitions(spec.partitions())
                        .replicas(spec.replicas())
                        .configs(spec.configs())
                        .build())
                .toArray(NewTopic[]::new);
        return new KafkaAdmin.NewTopics(topics);
    }

    /**
     * A plain Admin client for the operations endpoints. It reuses the
     * connection settings Spring Boot built for KafkaAdmin (spring.kafka.*),
     * including SASL when the "shared" profile is active. It is the same Java
     * API the kafka-topics.sh and kafka-configs.sh tools call.
     */
    @Bean(destroyMethod = "close")
    Admin adminClient(KafkaAdmin kafkaAdmin) {
        return Admin.create(kafkaAdmin.getConfigurationProperties());
    }
}
