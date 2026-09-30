package com.training.kafka.telco.simulator;

import java.util.List;
import java.util.stream.IntStream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** The demo subscriber base: MSISDNs 966500000001, 966500000002, ... */
@Component
public class Subscribers {

    private final List<String> msisdns;

    public Subscribers(@Value("${telco.simulator.subscribers:20}") int count) {
        this.msisdns = IntStream.rangeClosed(1, count)
                .mapToObj(i -> String.format("966500%06d", i))
                .toList();
    }

    public List<String> msisdns() {
        return msisdns;
    }
}
