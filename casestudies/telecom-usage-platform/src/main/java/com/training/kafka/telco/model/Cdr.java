package com.training.kafka.telco.model;

import java.time.Instant;

/**
 * Call Detail Record: one billable usage event.
 *
 * The Kafka record KEY is always {@link #msisdn()}, the subscriber's phone
 * number. That single choice (Module 1, partitions and keys) gives:
 *  - all events of one subscriber land in one partition, so they are read in
 *    the order they happened (call-start is never rated after call-end);
 *  - one consumer thread owns a subscriber at a time, so per-subscriber state
 *    (running bill, fraud window) needs no cross-thread coordination.
 *
 * @param cdrId              unique id assigned by the network element
 * @param type               VOICE, SMS or DATA
 * @param msisdn             the subscriber (the Kafka key)
 * @param counterparty       called / texted number ("-" for data sessions)
 * @param destinationCountry ISO country of the counterparty ("SA" = domestic)
 * @param durationSec        call length in seconds (VOICE), otherwise 0
 * @param bytes              transferred bytes (DATA), otherwise 0
 * @param eventTime          when the usage happened (event time, not arrival time)
 * @param cellId             serving cell tower
 */
public record Cdr(String cdrId, CdrType type, String msisdn, String counterparty, String destinationCountry,
                  long durationSec, long bytes, Instant eventTime, String cellId) {

    /** Deliberately not named isX(): Jackson would then write it into the JSON as an extra field. */
    public boolean international() {
        return type != CdrType.DATA && !"SA".equals(destinationCountry);
    }
}
