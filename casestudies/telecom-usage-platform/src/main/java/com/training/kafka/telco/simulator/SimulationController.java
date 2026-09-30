package com.training.kafka.telco.simulator;

import java.util.Map;

import com.training.kafka.telco.simulator.TrafficSimulator.Status;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Buttons for the "network": produce usage records, telemetry and a fraud pattern. */
@RestController
@RequestMapping("/api/simulate")
public class SimulationController {

    private final TrafficSimulator simulator;

    public SimulationController(TrafficSimulator simulator) {
        this.simulator = simulator;
    }

    /** Returns how many records landed in each partition: keys spread subscribers over partitions. */
    @PostMapping("/cdr")
    public Map<String, Integer> cdr(@RequestParam(defaultValue = "100") int count) {
        return simulator.generateCdrs(count);
    }

    @PostMapping("/fraud-burst")
    public Map<String, Object> fraudBurst(@RequestParam(defaultValue = "966500000007") String msisdn,
                                          @RequestParam(defaultValue = "8") int calls) {
        return Map.of("msisdn", msisdn, "internationalCallsSent", simulator.fraudBurst(msisdn, calls));
    }

    @PostMapping("/telemetry")
    public Map<String, Integer> telemetry(@RequestParam(defaultValue = "1000") int count) {
        return Map.of("pointsSent", simulator.generateTelemetry(count));
    }

    @PostMapping("/start")
    public Status start(@RequestParam(defaultValue = "50") int rate) {
        simulator.start(rate);
        return simulator.status();
    }

    @PostMapping("/stop")
    public Status stop() {
        simulator.stop();
        return simulator.status();
    }

    @GetMapping
    public Status status() {
        return simulator.status();
    }
}
