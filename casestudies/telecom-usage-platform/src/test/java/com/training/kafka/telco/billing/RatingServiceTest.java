package com.training.kafka.telco.billing;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import com.training.kafka.telco.model.Cdr;
import com.training.kafka.telco.model.CdrType;
import com.training.kafka.telco.model.Charge;
import com.training.kafka.telco.model.Plan;
import org.junit.jupiter.api.Test;

class RatingServiceTest {

    private final RatingService rating = new RatingService();

    private static Cdr voice(long seconds, String country) {
        return new Cdr("c1", CdrType.VOICE, "966500000001", "966555555555", country, seconds, 0, Instant.now(), "CELL-1");
    }

    @Test
    void voiceIsBilledPerStartedMinute() {
        // 61 seconds = 2 started minutes; PREPAID_BASIC voice = 0.20 SAR/min
        Charge charge = rating.rate(voice(61, "SA"), Optional.of(Plan.PREPAID_BASIC), "t-0-1");

        assertThat(charge.quantity()).isEqualByComparingTo("2");
        assertThat(charge.amountSar()).isEqualByComparingTo("0.400");
        assertThat(charge.international()).isFalse();
    }

    @Test
    void internationalVoiceCostsFiveTimesMore() {
        Charge domestic = rating.rate(voice(60, "SA"), Optional.of(Plan.POSTPAID_STANDARD), "a");
        Charge abroad = rating.rate(voice(60, "PK"), Optional.of(Plan.POSTPAID_STANDARD), "b");

        assertThat(abroad.international()).isTrue();
        assertThat(abroad.amountSar()).isEqualByComparingTo(domestic.amountSar().multiply(BigDecimal.valueOf(5)));
    }

    @Test
    void dataIsBilledPerMegabyte() {
        Cdr data = new Cdr("c2", CdrType.DATA, "966500000001", "-", "SA", 0, 10L * 1_048_576, Instant.now(), "CELL-1");

        Charge charge = rating.rate(data, Optional.of(Plan.POSTPAID_PREMIUM), "d");

        assertThat(charge.quantity()).isEqualByComparingTo("10");
        assertThat(charge.amountSar()).isEqualByComparingTo("0.050");      // 10 MB x 0.005
    }

    @Test
    void unknownSubscriberIsRatedOnTheDefaultPlanAndFlagged() {
        Charge charge = rating.rate(voice(60, "SA"), Optional.empty(), "x");

        assertThat(charge.plan()).isEqualTo(RatingService.DEFAULT_PLAN);
        assertThat(charge.planKnown()).isFalse();
    }

    @Test
    void sameCdrAlwaysProducesTheSameChargeId() {
        // The id is passed in by the consumer as topic-partition-offset; rating must not invent its own.
        assertThat(rating.rate(voice(10, "SA"), Optional.empty(), "cdr.voice-3-42").chargeId())
                .isEqualTo("cdr.voice-3-42");
    }
}
