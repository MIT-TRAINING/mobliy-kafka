package com.training.kafka.telco.plans;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.training.kafka.telco.model.PlanEvent;
import org.springframework.stereotype.Component;

/**
 * In-memory "table" of the current plan per subscriber, built by replaying the
 * compacted topic subscriber.plan from offset 0 (see PlanCacheConsumer).
 *
 * This is why compaction exists (Module 1 section 7.3, Module 3 section 5.5):
 * a new instance of the service can rebuild the full state from Kafka alone.
 * No database dump, no snapshot file. The topic IS the table's durable form.
 */
@Component
public class PlanTable {

    private final Map<String, PlanEvent> plans = new ConcurrentHashMap<>();
    private final AtomicLong recordsRead = new AtomicLong();
    private final AtomicLong tombstonesRead = new AtomicLong();

    void apply(String msisdn, PlanEvent event) {
        recordsRead.incrementAndGet();
        if (event == null) {                      // tombstone: the subscriber left
            tombstonesRead.incrementAndGet();
            plans.remove(msisdn);
        } else {
            plans.put(msisdn, event);
        }
    }

    public Optional<PlanEvent> find(String msisdn) {
        return Optional.ofNullable(plans.get(msisdn));
    }

    public List<PlanEvent> all() {
        return plans.values().stream().sorted(Comparator.comparing(PlanEvent::msisdn)).toList();
    }

    /** Records read from the topic since this application started (the cost of rebuilding the table). */
    public long recordsRead() {
        return recordsRead.get();
    }

    public long tombstonesRead() {
        return tombstonesRead.get();
    }
}
