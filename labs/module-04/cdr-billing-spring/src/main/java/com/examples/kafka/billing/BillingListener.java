package com.examples.kafka.billing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The consumer side: the listener container runs the poll loop and commits
 * after each batch of records has been processed (AckMode.BATCH, at-least-once).
 * concurrency = 3 starts three consumers in the group ${lab.groups.billing}.
 *
 * Failures are handled by the DefaultErrorHandler in ErrorHandlingConfig:
 *   IllegalArgumentException  (bad data)        -> dead-letter topic at once
 *   BillingUnavailableException (DB is down)    -> 3 retries, 1 s apart, then dead-letter topic
 */
@Component
public class BillingListener {

    /** Simulates a subscriber whose billing-DB writes always time out. */
    static final String UNLUCKY_MSISDN = "966500000099";

    private static final Logger log = LoggerFactory.getLogger(BillingListener.class);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @KafkaListener(id = "billing", topics = "${lab.topics.cdr}", groupId = "${lab.groups.billing}",
            concurrency = "${lab.concurrency}")
    public void onCdr(ConsumerRecord<String, String> record) {
        JsonNode cdr;
        try {
            cdr = JSON.readTree(record.value());
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Not a JSON CDR: " + record.value());
        }
        if (!cdr.hasNonNull("cdrId") || !cdr.hasNonNull("msisdn")) {
            throw new IllegalArgumentException("CDR without cdrId/msisdn: " + record.value());
        }
        if (UNLUCKY_MSISDN.equals(cdr.get("msisdn").asString())) {
            throw new BillingUnavailableException("Billing DB timeout for " + UNLUCKY_MSISDN + " (simulated)");
        }
        log.info("Billed p={} off={} key={} durationSec={}",
                record.partition(), record.offset(), record.key(), cdr.path("durationSec").asInt());
    }

    /** A transient failure: worth retrying. */
    static class BillingUnavailableException extends RuntimeException {
        BillingUnavailableException(String message) {
            super(message);
        }
    }
}
