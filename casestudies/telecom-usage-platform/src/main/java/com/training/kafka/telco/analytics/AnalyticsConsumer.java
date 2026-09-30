package com.training.kafka.telco.analytics;

import com.training.kafka.telco.common.JsonCodec;
import com.training.kafka.telco.model.Cdr;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Usage analytics: a THIRD independent group on the CDR topics.
 *
 * A group is just a name. Starting a new group with auto.offset.reset=earliest
 * replays all retained CDRs (up to the topic's retention.ms) with no change
 * to any other application. Compare that with a queue (IBM MQ), where a message
 * consumed by billing is gone and a new consumer sees only new messages.
 */
@Component
public class AnalyticsConsumer {

    static final String LISTENER_ID = "usage-analytics";

    private static final Logger log = LoggerFactory.getLogger(AnalyticsConsumer.class);

    private final JsonCodec json;
    private final UsageAnalytics analytics;

    public AnalyticsConsumer(JsonCodec json, UsageAnalytics analytics) {
        this.json = json;
        this.analytics = analytics;
    }

    @KafkaListener(id = LISTENER_ID, groupId = "#{topics.group('usage-analytics')}", concurrency = "2",
            topics = {"#{topics.voice()}", "#{topics.sms()}", "#{topics.data()}"})
    public void onCdr(ConsumerRecord<String, String> record) {
        try {
            analytics.record(record.topic(), record.partition(), json.fromJson(record.value(), Cdr.class));
        } catch (RuntimeException e) {
            log.warn("Skipping unreadable record {}-{}@{}: {}", record.topic(), record.partition(),
                    record.offset(), e.getMessage());
        }
    }
}
