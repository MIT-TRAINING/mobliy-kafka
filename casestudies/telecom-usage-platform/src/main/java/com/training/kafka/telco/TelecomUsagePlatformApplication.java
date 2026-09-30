package com.training.kafka.telco;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * TelcoPulse: real-time usage, billing and fraud platform for a telecom operator.
 *
 * One Spring Boot application plays every role so a class can see the whole
 * data flow in a single log and a single dashboard:
 *
 *   traffic simulator --> cdr.voice / cdr.sms / cdr.data --> billing, fraud, analytics
 *   plan management   --> subscriber.plan (compacted)    --> plan cache used by billing
 *   billing           --> billing.charges
 *   network probes    --> network.telemetry
 *   operations        --> audit.events
 *
 * In production these would be separate services owned by separate teams.
 * Kafka is what lets them stay separate.
 */
@SpringBootApplication
public class TelecomUsagePlatformApplication {

    public static void main(String[] args) {
        SpringApplication.run(TelecomUsagePlatformApplication.class, args);
    }
}
