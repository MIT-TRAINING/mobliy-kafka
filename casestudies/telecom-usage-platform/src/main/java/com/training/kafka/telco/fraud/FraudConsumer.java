package com.training.kafka.telco.fraud;

import com.training.kafka.telco.common.JsonCodec;
import com.training.kafka.telco.model.Cdr;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Fraud detection: a SECOND, independent consumer group on cdr.voice.
 *
 * Billing and fraud read the same topic under different group ids, so each gets
 * every record and each has its own committed offsets and its own lag. A slow
 * billing system cannot delay fraud detection, and adding fraud detection did
 * not require touching billing or the producers. That decoupling is the reason
 * telecoms put Kafka between network elements and their consumers.
 */
@Component
public class FraudConsumer {

    static final String LISTENER_ID = "fraud-detection";

    private static final Logger log = LoggerFactory.getLogger(FraudConsumer.class);

    private final JsonCodec json;
    private final FraudService fraud;

    public FraudConsumer(JsonCodec json, FraudService fraud) {
        this.json = json;
        this.fraud = fraud;
    }

    @KafkaListener(id = LISTENER_ID, groupId = "#{topics.group('fraud-detection')}", concurrency = "3",
            topics = "#{topics.voice()}")
    public void onVoiceCdr(ConsumerRecord<String, String> record) {
        try {
            fraud.inspect(json.fromJson(record.value(), Cdr.class));
        } catch (RuntimeException e) {
            log.warn("Skipping unreadable record {}-{}@{}: {}", record.topic(), record.partition(),
                    record.offset(), e.getMessage());
        }
    }
}
