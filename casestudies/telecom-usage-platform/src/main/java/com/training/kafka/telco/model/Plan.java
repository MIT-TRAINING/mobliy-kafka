package com.training.kafka.telco.model;

import java.math.BigDecimal;

/** Tariff plans and their unit prices in SAR. */
public enum Plan {

    //            voice / min   SMS each   data / MB
    PREPAID_BASIC("0.20", "0.10", "0.050"),
    POSTPAID_STANDARD("0.12", "0.05", "0.020"),
    POSTPAID_PREMIUM("0.05", "0.00", "0.005");

    private final BigDecimal voicePerMinute;
    private final BigDecimal smsEach;
    private final BigDecimal dataPerMb;

    Plan(String voicePerMinute, String smsEach, String dataPerMb) {
        this.voicePerMinute = new BigDecimal(voicePerMinute);
        this.smsEach = new BigDecimal(smsEach);
        this.dataPerMb = new BigDecimal(dataPerMb);
    }

    public BigDecimal voicePerMinute() {
        return voicePerMinute;
    }

    public BigDecimal smsEach() {
        return smsEach;
    }

    public BigDecimal dataPerMb() {
        return dataPerMb;
    }
}
