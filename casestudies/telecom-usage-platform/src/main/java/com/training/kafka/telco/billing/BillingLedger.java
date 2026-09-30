package com.training.kafka.telco.billing;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;

/**
 * Running bill per subscriber plus counters, kept in memory for the demo.
 * It is safe to update from several consumer threads at once. A real system
 * would write to a database; the point here is which thread sees which
 * subscriber, not persistence.
 */
@Component
public class BillingLedger {

    private final Map<String, BigDecimal> totalBySubscriber = new ConcurrentHashMap<>();
    private final AtomicLong rated = new AtomicLong();
    private final AtomicLong chargesPublished = new AtomicLong();
    private final AtomicLong chargesFailed = new AtomicLong();
    private final AtomicLong unreadable = new AtomicLong();
    private final AtomicLong unknownPlan = new AtomicLong();
    private volatile long processingDelayMs;

    void add(String msisdn, BigDecimal amount, boolean planKnown) {
        totalBySubscriber.merge(msisdn, amount, BigDecimal::add);
        rated.incrementAndGet();
        if (!planKnown) {
            unknownPlan.incrementAndGet();
        }
    }

    void published() { chargesPublished.incrementAndGet(); }
    void failed() { chargesFailed.incrementAndGet(); }
    void unreadable() { unreadable.incrementAndGet(); }

    public long processingDelayMs() { return processingDelayMs; }

    /** Simulates a slow downstream (database, rating engine) so consumer lag becomes visible. */
    public void processingDelayMs(long millis) { this.processingDelayMs = Math.max(0, millis); }

    public Summary summary(int top) {
        List<Map.Entry<String, BigDecimal>> highest = totalBySubscriber.entrySet().stream()
                .sorted(Map.Entry.<String, BigDecimal>comparingByValue(Comparator.reverseOrder()))
                .limit(top)
                .toList();
        BigDecimal total = totalBySubscriber.values().stream().reduce(BigDecimal.ZERO, BigDecimal::add);
        return new Summary(rated.get(), chargesPublished.get(), chargesFailed.get(), unreadable.get(),
                unknownPlan.get(), processingDelayMs, total, highest.stream()
                        .map(e -> new SubscriberTotal(e.getKey(), e.getValue())).toList());
    }

    public record SubscriberTotal(String msisdn, BigDecimal totalSar) {}

    public record Summary(long cdrsRated, long chargesPublished, long chargesFailed, long unreadableRecords,
                          long ratedWithoutPlan, long processingDelayMs, BigDecimal totalSar,
                          List<SubscriberTotal> topSubscribers) {}
}
