package com.training.kafka.telco.billing;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Optional;

import com.training.kafka.telco.model.Cdr;
import com.training.kafka.telco.model.Charge;
import com.training.kafka.telco.model.Plan;
import org.springframework.stereotype.Service;

/**
 * Turns a usage record into money. Pure business logic, no Kafka: easy to unit
 * test, and identical whether it runs in a consumer, a batch job or a test.
 */
@Service
public class RatingService {

    private static final BigDecimal BYTES_PER_MB = BigDecimal.valueOf(1_048_576);
    private static final BigDecimal INTERNATIONAL_VOICE_FACTOR = BigDecimal.valueOf(5);
    private static final BigDecimal INTERNATIONAL_SMS_FACTOR = BigDecimal.valueOf(3);

    /** Used when a subscriber has no plan record (yet): bill at the entry-level tariff and say so. */
    static final Plan DEFAULT_PLAN = Plan.PREPAID_BASIC;

    public Charge rate(Cdr cdr, Optional<Plan> knownPlan, String chargeId) {
        Plan plan = knownPlan.orElse(DEFAULT_PLAN);
        boolean international = cdr.international();

        BigDecimal quantity;
        String unit;
        BigDecimal amount;

        switch (cdr.type()) {
            case VOICE -> {
                long minutes = (cdr.durationSec() + 59) / 60;           // billed per started minute
                quantity = BigDecimal.valueOf(minutes);
                unit = "minutes";
                amount = quantity.multiply(plan.voicePerMinute());
                if (international) {
                    amount = amount.multiply(INTERNATIONAL_VOICE_FACTOR);
                }
            }
            case SMS -> {
                quantity = BigDecimal.ONE;
                unit = "messages";
                amount = plan.smsEach();
                if (international) {
                    amount = amount.multiply(INTERNATIONAL_SMS_FACTOR);
                }
            }
            case DATA -> {
                quantity = BigDecimal.valueOf(cdr.bytes()).divide(BYTES_PER_MB, 4, RoundingMode.HALF_UP);
                unit = "MB";
                amount = quantity.multiply(plan.dataPerMb());
            }
            default -> throw new IllegalStateException("Unknown CDR type " + cdr.type());
        }

        return new Charge(chargeId, cdr.msisdn(), cdr.type(), plan, knownPlan.isPresent(), international,
                quantity, unit, amount.setScale(3, RoundingMode.HALF_UP), Instant.now());
    }
}
