package com.training.kafka.telco.fraud;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import com.training.kafka.telco.common.AuditPublisher;
import com.training.kafka.telco.common.JsonCodec;
import com.training.kafka.telco.model.Cdr;
import com.training.kafka.telco.model.FraudAlert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/** Holds the detector, the recent alerts and the counters shown on the dashboard. */
@Service
public class FraudService {

    private static final Logger log = LoggerFactory.getLogger(FraudService.class);
    private static final int KEEP_ALERTS = 50;

    private final FraudDetector detector;
    private final AuditPublisher audit;
    private final JsonCodec json;
    private final List<FraudAlert> alerts = new CopyOnWriteArrayList<>();
    private final AtomicLong inspected = new AtomicLong();

    public FraudService(@Value("${telco.fraud.threshold:5}") int threshold,
                        @Value("${telco.fraud.window-seconds:60}") long windowSeconds,
                        AuditPublisher audit, JsonCodec json) {
        this.detector = new FraudDetector(threshold, Duration.ofSeconds(windowSeconds));
        this.audit = audit;
        this.json = json;
    }

    void inspect(Cdr cdr) {
        inspected.incrementAndGet();
        detector.observe(cdr).ifPresent(alert -> {
            alerts.add(alert);
            if (alerts.size() > KEEP_ALERTS) {
                alerts.remove(0);
            }
            log.warn("FRAUD ALERT {} made {} international calls within {}s",
                    alert.msisdn(), alert.internationalCalls(), alert.windowSeconds());
            audit.publish("FRAUD_ALERT", alert.msisdn(), json.toJson(alert));
        });
    }

    public Summary summary() {
        List<FraudAlert> newestFirst = new ArrayList<>(alerts);
        Collections.reverse(newestFirst);
        return new Summary(inspected.get(), newestFirst.size(), newestFirst);
    }

    public record Summary(long voiceCdrsInspected, int alertCount, List<FraudAlert> recentAlerts) {}
}
