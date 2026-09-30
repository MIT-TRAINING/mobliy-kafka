package com.training.kafka.telco.model;

import java.time.Instant;

/**
 * The CURRENT plan of a subscriber: the value of a record in the compacted
 * topic subscriber.plan (key = msisdn).
 *
 * A record with a null value (a tombstone) means the subscriber left the network.
 */
public record PlanEvent(String msisdn, Plan plan, Instant effectiveFrom) {
}
