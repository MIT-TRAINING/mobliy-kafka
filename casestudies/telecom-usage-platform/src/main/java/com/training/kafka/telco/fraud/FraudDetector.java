package com.training.kafka.telco.fraud;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.training.kafka.telco.model.Cdr;
import com.training.kafka.telco.model.CdrType;
import com.training.kafka.telco.model.FraudAlert;

/**
 * Rule: {@code threshold} international calls by one subscriber inside a
 * sliding {@code window} of EVENT time raises one alert.
 *
 * The rule keeps a small window of state per subscriber and is only correct if
 * the calls of a subscriber are seen in order. Kafka guarantees exactly that
 * within a partition, and the producers use the MSISDN as the key. Had they
 * used a random key, calls of one subscriber would be spread over consumers
 * and this rule would silently miss fraud. Choosing the key is a design
 * decision about correctness, not only about balance.
 */
public class FraudDetector {

    private final int threshold;
    private final Duration window;
    private final Map<String, Deque<Instant>> callsBySubscriber = new ConcurrentHashMap<>();
    private final Map<String, Instant> lastAlertBySubscriber = new ConcurrentHashMap<>();

    public FraudDetector(int threshold, Duration window) {
        this.threshold = threshold;
        this.window = window;
    }

    public Optional<FraudAlert> observe(Cdr cdr) {
        if (cdr.type() != CdrType.VOICE || !cdr.international()) {
            return Optional.empty();
        }
        Deque<Instant> calls = callsBySubscriber.computeIfAbsent(cdr.msisdn(), k -> new ArrayDeque<>());
        synchronized (calls) {
            Instant now = cdr.eventTime();
            calls.addLast(now);
            while (!calls.isEmpty() && calls.peekFirst().isBefore(now.minus(window))) {
                calls.removeFirst();
            }
            Instant lastAlert = lastAlertBySubscriber.get(cdr.msisdn());
            boolean coolingDown = lastAlert != null && !now.isAfter(lastAlert.plus(window));
            if (calls.size() >= threshold && !coolingDown) {
                lastAlertBySubscriber.put(cdr.msisdn(), now);
                return Optional.of(new FraudAlert(cdr.msisdn(), calls.size(), window.toSeconds(),
                        calls.peekFirst(), now, Instant.now()));
            }
        }
        return Optional.empty();
    }
}
