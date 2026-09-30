package com.training.kafka.telco.plans;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.training.kafka.telco.common.AuditPublisher;
import com.training.kafka.telco.common.JsonCodec;
import com.training.kafka.telco.model.Plan;
import com.training.kafka.telco.model.PlanEvent;
import com.training.kafka.telco.simulator.Subscribers;
import com.training.kafka.telco.topics.TopicCatalog;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/** Writes subscriber state to the compacted topic. Key = MSISDN, always. */
@Service
public class PlanService {

    private final KafkaTemplate<String, String> template;
    private final JsonCodec json;
    private final TopicCatalog topics;
    private final Subscribers subscribers;
    private final AuditPublisher audit;

    public PlanService(KafkaTemplate<String, String> template, JsonCodec json, TopicCatalog topics,
                       Subscribers subscribers, AuditPublisher audit) {
        this.template = template;
        this.json = json;
        this.topics = topics;
        this.subscribers = subscribers;
        this.audit = audit;
    }

    /** kafka-console-producer with a key: "966500000001:{...plan json...}" */
    public void changePlan(String msisdn, Plan plan) throws Exception {
        send(msisdn, plan).get(10, TimeUnit.SECONDS);
        audit.publish("PLAN_CHANGED", msisdn, "plan=" + plan);
    }

    /** A record with a null value: "this key no longer exists". Compaction later removes the key. */
    public void terminate(String msisdn) throws Exception {
        template.send(topics.plan(), msisdn, null).get(10, TimeUnit.SECONDS);
        audit.publish("SUBSCRIBER_TERMINATED", msisdn, "tombstone written to " + topics.plan());
    }

    /** Give every demo subscriber a starting plan (round robin over the three plans). */
    public int seed() {
        List<String> msisdns = subscribers.msisdns();
        Plan[] plans = Plan.values();
        CompletableFuture<?>[] sends = new CompletableFuture<?>[msisdns.size()];
        for (int i = 0; i < msisdns.size(); i++) {
            sends[i] = send(msisdns.get(i), plans[i % plans.length]);
        }
        CompletableFuture.allOf(sends).join();
        return sends.length;
    }

    /**
     * Rewrite every subscriber's plan {@code rounds} times. The topic then holds
     * subscribers x rounds records, but only one per subscriber is still current:
     * exactly the "dirty" data the log cleaner exists to remove.
     */
    public int churn(int rounds) {
        List<String> msisdns = subscribers.msisdns();
        Plan[] plans = Plan.values();
        int sent = 0;
        CompletableFuture<?>[] sends = new CompletableFuture<?>[msisdns.size() * rounds];
        for (int r = 0; r < rounds; r++) {
            for (int i = 0; i < msisdns.size(); i++) {
                sends[sent++] = send(msisdns.get(i), plans[(i + r) % plans.length]);
            }
        }
        CompletableFuture.allOf(sends).join();
        return sent;
    }

    private CompletableFuture<?> send(String msisdn, Plan plan) {
        String value = json.toJson(new PlanEvent(msisdn, plan, Instant.now()));
        return template.send(topics.plan(), msisdn, value);
    }
}
