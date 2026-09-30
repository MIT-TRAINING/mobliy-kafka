package com.training.kafka.telco.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

/** CDRs travel as JSON text, so the wire format must round-trip and stay clean. */
class CdrJsonTest {

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void cdrRoundTripsThroughJson() {
        Cdr cdr = new Cdr("id-1", CdrType.VOICE, "966500000001", "966555555555", "PK", 90, 0,
                Instant.parse("2026-01-01T10:00:00Z"), "CELL-0007");

        String json = mapper.writeValueAsString(cdr);

        assertThat(mapper.readValue(json, Cdr.class)).isEqualTo(cdr);
        // derived values must not leak into the wire format
        assertThat(json).doesNotContain("international");
    }

    @Test
    void planEventRoundTrips() {
        PlanEvent event = new PlanEvent("966500000001", Plan.POSTPAID_PREMIUM, Instant.parse("2026-01-01T10:00:00Z"));

        assertThat(mapper.readValue(mapper.writeValueAsString(event), PlanEvent.class)).isEqualTo(event);
    }
}
