package com.training.kafka.telco.plans;

import java.util.List;
import java.util.Map;

import com.training.kafka.telco.model.Plan;
import com.training.kafka.telco.model.PlanEvent;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** Subscriber plan management (state on a compacted topic). */
@RestController
@RequestMapping("/api/plans")
public class PlanController {

    private final PlanService service;
    private final PlanTable table;

    public PlanController(PlanService service, PlanTable table) {
        this.service = service;
        this.table = table;
    }

    @GetMapping
    public PlansView plans() {
        List<PlanEvent> all = table.all();
        return new PlansView(all.size(), table.recordsRead(), table.tombstonesRead(), all);
    }

    @PutMapping("/{msisdn}")
    public Map<String, String> change(@PathVariable String msisdn, @RequestParam Plan plan) throws Exception {
        service.changePlan(msisdn, plan);
        return Map.of("msisdn", msisdn, "plan", plan.name());
    }

    @DeleteMapping("/{msisdn}")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, String> terminate(@PathVariable String msisdn) throws Exception {
        service.terminate(msisdn);
        return Map.of("msisdn", msisdn, "written", "tombstone (null value)");
    }

    @PostMapping("/seed")
    public Map<String, Integer> seed() {
        return Map.of("recordsWritten", service.seed());
    }

    @PostMapping("/churn")
    public Map<String, Integer> churn(@RequestParam(defaultValue = "5") int rounds) {
        return Map.of("recordsWritten", service.churn(rounds));
    }

    /**
     * @param subscribers      subscribers currently in the in-memory table
     * @param recordsReadSinceStart records this instance read from the topic since it started
     */
    public record PlansView(int subscribers, long recordsReadSinceStart, long tombstonesReadSinceStart,
                            List<PlanEvent> plans) {}
}
