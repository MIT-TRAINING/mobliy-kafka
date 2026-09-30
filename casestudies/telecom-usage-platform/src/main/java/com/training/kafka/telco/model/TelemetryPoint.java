package com.training.kafka.telco.model;

import java.time.Instant;

/** A radio-network measurement from a cell tower. High volume, low value per record. */
public record TelemetryPoint(String cellId, Instant at, int activeUsers, double prbUtilisationPct,
                             double throughputMbps, int dropRatePerMille) {
}
