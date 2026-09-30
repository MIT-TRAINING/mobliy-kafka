package com.training.kafka.telco.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/**
 * Builds plain, throw-away consumers for the "look at the topic" tools
 * (replay statistics, the durability drill). They use assign(), not subscribe(),
 * never join a consumer group and never commit offsets, so they do not disturb
 * the real groups. It is the Java equivalent of
 *   kafka-console-consumer.sh --partition N --offset earliest
 */
@Component
public class KafkaConnection {

    private final KafkaAdmin kafkaAdmin;

    public KafkaConnection(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    public KafkaConsumer<String, String> newReader() {
        // Connection + security settings (bootstrap servers, SASL) from spring.kafka.*
        Map<String, Object> props = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "telco-reader-" + System.nanoTime());
        return new KafkaConsumer<>(props);
    }
}
