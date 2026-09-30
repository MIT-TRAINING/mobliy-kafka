package com.training.kafka.telco.common;

import java.time.Instant;

import com.training.kafka.telco.model.AuditEvent;
import com.training.kafka.telco.topics.TopicCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Writes to audit.events (90-day retention, min.insync.replicas=2, acks=all). */
@Component
public class AuditPublisher {

    private static final Logger log = LoggerFactory.getLogger(AuditPublisher.class);

    private final KafkaTemplate<String, String> template;
    private final JsonCodec json;
    private final TopicCatalog topics;

    public AuditPublisher(KafkaTemplate<String, String> template, JsonCodec json, TopicCatalog topics) {
        this.template = template;
        this.json = json;
        this.topics = topics;
    }

    /** @param subject the key: what the event is about (a topic name, an MSISDN, ...) */
    public void publish(String action, String subject, String detail) {
        AuditEvent event = new AuditEvent(action, subject, detail, Instant.now());
        template.send(topics.audit(), subject, json.toJson(event)).whenComplete((result, error) -> {
            if (error != null) {
                log.error("Audit event LOST ({} {}): {}", action, subject, error.toString());
            }
        });
    }
}
