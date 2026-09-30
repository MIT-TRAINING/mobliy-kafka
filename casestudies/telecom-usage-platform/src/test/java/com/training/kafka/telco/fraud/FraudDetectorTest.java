package com.training.kafka.telco.fraud;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import com.training.kafka.telco.model.Cdr;
import com.training.kafka.telco.model.CdrType;
import com.training.kafka.telco.model.FraudAlert;
import org.junit.jupiter.api.Test;

class FraudDetectorTest {

    private static final Instant T0 = Instant.parse("2026-01-01T10:00:00Z");

    private static Cdr call(String msisdn, String country, Instant at) {
        return new Cdr("id", CdrType.VOICE, msisdn, "00", country, 30, 0, at, "CELL-1");
    }

    @Test
    void alertsWhenThresholdIsReachedInsideTheWindow() {
        FraudDetector detector = new FraudDetector(3, Duration.ofSeconds(60));

        assertThat(detector.observe(call("A", "PK", T0))).isEmpty();
        assertThat(detector.observe(call("A", "PK", T0.plusSeconds(10)))).isEmpty();
        Optional<FraudAlert> alert = detector.observe(call("A", "PK", T0.plusSeconds(20)));

        assertThat(alert).isPresent();
        assertThat(alert.get().msisdn()).isEqualTo("A");
        assertThat(alert.get().internationalCalls()).isEqualTo(3);
    }

    @Test
    void callsOutsideTheWindowDoNotCount() {
        FraudDetector detector = new FraudDetector(3, Duration.ofSeconds(60));

        detector.observe(call("A", "PK", T0));
        detector.observe(call("A", "PK", T0.plusSeconds(100)));
        Optional<FraudAlert> alert = detector.observe(call("A", "PK", T0.plusSeconds(200)));

        assertThat(alert).isEmpty();
    }

    @Test
    void domesticCallsAndOtherSubscribersAreIgnored() {
        FraudDetector detector = new FraudDetector(2, Duration.ofSeconds(60));

        detector.observe(call("A", "SA", T0));
        detector.observe(call("A", "SA", T0.plusSeconds(1)));
        detector.observe(call("B", "PK", T0.plusSeconds(2)));

        assertThat(detector.observe(call("A", "SA", T0.plusSeconds(3)))).isEmpty();
    }

    @Test
    void oneBurstRaisesOneAlertNotOnePerCall() {
        FraudDetector detector = new FraudDetector(3, Duration.ofSeconds(60));
        int alerts = 0;
        for (int i = 0; i < 10; i++) {
            if (detector.observe(call("A", "PK", T0.plusSeconds(i))).isPresent()) {
                alerts++;
            }
        }
        assertThat(alerts).isEqualTo(1);
    }
}
