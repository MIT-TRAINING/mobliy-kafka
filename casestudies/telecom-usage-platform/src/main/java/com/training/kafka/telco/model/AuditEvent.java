package com.training.kafka.telco.model;

import java.time.Instant;

/** Something an operator or the system did that compliance may ask about later. */
public record AuditEvent(String action, String subject, String detail, Instant at) {
}
