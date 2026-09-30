package com.training.kafka.telco.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A rated usage event, published to billing.charges (key = msisdn).
 *
 * {@link #chargeId()} is "topic-partition-offset" of the CDR it came from. The
 * billing consumer is at-least-once (Module 4 makes that precise), so after a
 * crash or rebalance the same CDR can be rated twice. A downstream system can
 * discard duplicates because the same CDR always produces the same chargeId.
 */
public record Charge(String chargeId, String msisdn, CdrType type, Plan plan, boolean planKnown,
                     boolean international, BigDecimal quantity, String unit, BigDecimal amountSar,
                     Instant ratedAt) {
}
