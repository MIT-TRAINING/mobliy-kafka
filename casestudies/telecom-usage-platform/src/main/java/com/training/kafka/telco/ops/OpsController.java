package com.training.kafka.telco.ops;

import java.util.List;
import java.util.Map;

import com.training.kafka.telco.analytics.UsageAnalytics;
import com.training.kafka.telco.billing.BillingLedger;
import com.training.kafka.telco.fraud.FraudService;
import com.training.kafka.telco.ops.OpsService.ClusterView;
import com.training.kafka.telco.ops.OpsService.GroupView;
import com.training.kafka.telco.ops.OpsService.OffsetsView;
import com.training.kafka.telco.ops.OpsService.StorageView;
import com.training.kafka.telco.ops.OpsService.TopicView;
import com.training.kafka.telco.ops.ReplayReader.ReplayStats;
import com.training.kafka.telco.ops.DurabilityDrill.DrillState;
import com.training.kafka.telco.ops.DurabilityDrill.WriteResult;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST front end for the operator tools (Kafka Admin API) and the application's own counters.
 * Errors are turned into JSON by ApiErrorHandler.
 */
@RestController
@RequestMapping("/api")
public class OpsController {

    private final OpsService ops;
    private final ReplayReader replay;
    private final DurabilityDrill drill;
    private final ListenerControl listeners;
    private final BillingLedger billing;
    private final FraudService fraud;
    private final UsageAnalytics analytics;

    public OpsController(OpsService ops, ReplayReader replay, DurabilityDrill drill, ListenerControl listeners,
                         BillingLedger billing, FraudService fraud, UsageAnalytics analytics) {
        this.ops = ops;
        this.replay = replay;
        this.drill = drill;
        this.listeners = listeners;
        this.billing = billing;
        this.fraud = fraud;
        this.analytics = analytics;
    }

    // ------------------------------------------------------------ Kafka side

    @GetMapping("/ops/cluster")
    public ClusterView cluster() throws Exception {
        return ops.cluster();
    }

    @GetMapping("/ops/topics")
    public List<TopicView> topics() throws Exception {
        return ops.topics();
    }

    /** kafka-configs.sh --alter --add-config: try retention.ms, segment.bytes, min.insync.replicas ... */
    @PutMapping("/ops/topics/{topic}/configs/{key}")
    public TopicView setConfig(@PathVariable String topic, @PathVariable String key, @RequestParam String value)
            throws Exception {
        return ops.setConfig(topic, key, value);
    }

    /** kafka-configs.sh --alter --delete-config */
    @DeleteMapping("/ops/topics/{topic}/configs/{key}")
    public TopicView resetConfig(@PathVariable String topic, @PathVariable String key) throws Exception {
        return ops.resetConfig(topic, key);
    }

    @GetMapping("/ops/storage")
    public List<StorageView> storage() throws Exception {
        return ops.storage();
    }

    @GetMapping("/ops/topics/{topic}/offsets")
    public OffsetsView offsets(@PathVariable String topic) throws Exception {
        return ops.offsets(topic);
    }

    @GetMapping("/ops/topics/{topic}/replay")
    public ReplayStats replay(@PathVariable String topic) {
        return replay.replay(topic);
    }

    @GetMapping("/ops/groups")
    public List<GroupView> groups() throws Exception {
        return ops.groups();
    }

    // --------------------------------------------------- consumers of the app

    @GetMapping("/listeners")
    public Map<String, Boolean> listeners() {
        return listeners.status();
    }

    @PostMapping("/listeners/{id}/stop")
    public Map<String, Boolean> stop(@PathVariable String id) {
        return listeners.stop(id);
    }

    @PostMapping("/listeners/{id}/start")
    public Map<String, Boolean> start(@PathVariable String id) {
        return listeners.start(id);
    }

    /** Make billing slow (ms per record) to create consumer lag; 0 = full speed. */
    @PostMapping("/billing/delay")
    public Map<String, Long> delay(@RequestParam long ms) {
        billing.processingDelayMs(ms);
        return Map.of("processingDelayMs", billing.processingDelayMs());
    }

    @GetMapping("/insights/billing")
    public BillingLedger.Summary billing() {
        return billing.summary(5);
    }

    @GetMapping("/insights/fraud")
    public FraudService.Summary fraud() {
        return fraud.summary();
    }

    @GetMapping("/insights/analytics")
    public UsageAnalytics.Summary analytics() {
        return analytics.summary(5);
    }

    // ------------------------------------------------------ durability drill

    @PostMapping("/drill/write")
    public WriteResult drillWrite(@RequestParam String acks, @RequestParam(defaultValue = "test-record") String payload) {
        return drill.write(acks, payload);
    }

    @GetMapping("/drill/state")
    public DrillState drillState() throws Exception {
        return drill.state();
    }
}
