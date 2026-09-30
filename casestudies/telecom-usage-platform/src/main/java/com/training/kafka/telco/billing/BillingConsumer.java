package com.training.kafka.telco.billing;

import com.training.kafka.telco.common.JsonCodec;
import com.training.kafka.telco.model.Cdr;
import com.training.kafka.telco.model.Charge;
import com.training.kafka.telco.model.PlanEvent;
import com.training.kafka.telco.plans.PlanTable;
import com.training.kafka.telco.topics.TopicCatalog;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Billing: reads every CDR, rates it with the subscriber's current plan and
 * publishes the charge.
 *
 * Kafka features at work
 *  - consumer group: 3 threads (concurrency = 3) share the 15 partitions of the
 *    three CDR topics, so billing scales by adding threads or instances (up to the partition count);
 *  - key = MSISDN: every record of one subscriber arrives here in order, on one thread;
 *  - offsets: progress is committed per group, so a restart continues where it stopped;
 *  - lag: switch on the slow mode in the dashboard and watch the gap between
 *    the log end offset and the group's committed offset grow.
 */
@Component
public class BillingConsumer {

    static final String LISTENER_ID = "billing";

    private static final Logger log = LoggerFactory.getLogger(BillingConsumer.class);

    private final JsonCodec json;
    private final RatingService rating;
    private final PlanTable plans;
    private final BillingLedger ledger;
    private final KafkaTemplate<String, String> template;
    private final TopicCatalog topics;

    public BillingConsumer(JsonCodec json, RatingService rating, PlanTable plans, BillingLedger ledger,
                           KafkaTemplate<String, String> template, TopicCatalog topics) {
        this.json = json;
        this.rating = rating;
        this.plans = plans;
        this.ledger = ledger;
        this.template = template;
        this.topics = topics;
    }

    @KafkaListener(id = LISTENER_ID, groupId = "#{topics.group('billing')}", concurrency = "3",
            topics = {"#{topics.voice()}", "#{topics.sms()}", "#{topics.data()}"})
    public void onCdr(ConsumerRecord<String, String> record) throws InterruptedException {
        long delay = ledger.processingDelayMs();
        if (delay > 0) {
            Thread.sleep(delay);
        }

        Cdr cdr;
        try {
            cdr = json.fromJson(record.value(), Cdr.class);
        } catch (RuntimeException e) {
            // Poison record. Skipping it keeps the partition moving; what a real
            // system does with it (dead-letter topic) is Module 9.
            ledger.unreadable();
            log.warn("Skipping unreadable record {}-{}@{}: {}", record.topic(), record.partition(),
                    record.offset(), e.getMessage());
            return;
        }

        String chargeId = record.topic() + "-" + record.partition() + "-" + record.offset();
        Charge charge = rating.rate(cdr, plans.find(cdr.msisdn()).map(PlanEvent::plan), chargeId);
        ledger.add(cdr.msisdn(), charge.amountSar(), charge.planKnown());

        // Key = MSISDN again: a subscriber's charges stay in order on billing.charges too.
        template.send(topics.charges(), cdr.msisdn(), json.toJson(charge)).whenComplete((result, error) -> {
            if (error == null) {
                ledger.published();
            } else {
                ledger.failed();
                log.error("Charge {} NOT published: {}", chargeId, error.toString());
            }
        });

        if (log.isDebugEnabled()) {
            log.debug("[{}] {}-{}@{} {} -> {} SAR", Thread.currentThread().getName(), record.topic(),
                    record.partition(), record.offset(), cdr.msisdn(), charge.amountSar());
        }
    }
}
