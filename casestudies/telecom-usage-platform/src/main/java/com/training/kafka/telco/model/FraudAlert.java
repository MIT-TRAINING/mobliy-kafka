package com.training.kafka.telco.model;

import java.time.Instant;

/** Raised when a subscriber makes too many international calls in a short event-time window. */
public record FraudAlert(String msisdn, int internationalCalls, long windowSeconds, Instant firstCallAt,
                         Instant lastCallAt, Instant detectedAt) {
}
