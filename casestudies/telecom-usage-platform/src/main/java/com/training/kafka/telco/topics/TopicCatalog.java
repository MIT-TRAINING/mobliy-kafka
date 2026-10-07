package com.training.kafka.telco.topics;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import com.training.kafka.telco.model.CdrType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Single source of truth for topic names, topic designs and consumer groups.
 *
 * This is the Module 3 idea in code: a Kafka cluster serves many kinds of data,
 * and broker defaults are only a baseline. Each topic gets an explicit
 * contract (section 4.1 "Workload class") and those settings live in version
 * control, not in someone's shell history. KafkaAdmin applies them at startup.
 *
 * The bean is named "topics" so listeners can use SpEL such as
 * {@code topics = "#{topics.voice()}"}, which is how the learner prefix reaches
 * annotation values.
 */
@Component("topics")
public class TopicCatalog {

    // ---- base names, without the learner prefix -------------------------------------------
    static final String VOICE = "cdr.voice";
    static final String SMS = "cdr.sms";
    static final String DATA = "cdr.data";
    static final String PLAN = "subscriber.plan";
    static final String CHARGES = "billing.charges";
    static final String TELEMETRY = "network.telemetry";
    static final String AUDIT = "audit.events";
    static final String DRILL = "drill.durability";

    // ---- setting values used more than once ------------------------------------------------
    private static final String DAYS_3 = "259200000";
    private static final String DAYS_7 = "604800000";
    private static final String DAYS_30 = "2592000000";
    private static final String DAYS_90 = "7776000000";

    private final String prefix;
    private final boolean labMode;
    private final boolean confluentCloud;

    public TopicCatalog(String prefix, boolean labMode) {
        this(prefix, labMode, false);
    }

    /**
     * @param confluentCloud true on Confluent Cloud (profile "ccloud"). Cloud fixes the replication
     *                       factor at 3, does not let you set compression.type or
     *                       min.cleanable.dirty.ratio, and enforces minimums such as segment.ms >= 10 min.
     *                       A topic declared outside those rules fails at startup with
     *                       PolicyViolationException, so the catalog adapts the affected contracts.
     */
    @Autowired
    public TopicCatalog(@Value("${telco.prefix:}") String prefix, @Value("${telco.lab-mode:true}") boolean labMode,
                        @Value("${telco.confluent-cloud:false}") boolean confluentCloud) {
        this.prefix = prefix;
        this.labMode = labMode;
        this.confluentCloud = confluentCloud;
    }

    // ---------------------------------------------------------------- names

    public String voice() { return prefix + VOICE; }
    public String sms() { return prefix + SMS; }
    public String data() { return prefix + DATA; }
    public String plan() { return prefix + PLAN; }
    public String charges() { return prefix + CHARGES; }
    public String telemetry() { return prefix + TELEMETRY; }
    public String audit() { return prefix + AUDIT; }
    public String drill() { return prefix + DRILL; }

    /** "0", "1", ... for subscriber.plan: the plan cache assigns itself every partition (see PlanCacheConsumer). */
    public List<String> planPartitions() {
        return IntStream.range(0, planSpec().partitions()).mapToObj(String::valueOf).toList();
    }

    public List<String> cdrTopics() {
        return List.of(voice(), sms(), data());
    }

    public String topicFor(CdrType type) {
        return switch (type) {
            case VOICE -> voice();
            case SMS -> sms();
            case DATA -> data();
        };
    }

    /** Consumer group id, prefixed like the topics (the shared cluster's ACLs are prefix based). */
    public String group(String name) {
        return prefix + name;
    }

    public boolean labMode() {
        return labMode;
    }

    public boolean confluentCloud() {
        return confluentCloud;
    }

    /** Accepts a full topic name or a base name such as "network.telemetry", and adds the prefix if needed. */
    public String resolve(String name) {
        return names().contains(name) ? name : prefix + name;
    }

    public List<String> names() {
        return specs().stream().map(TopicSpec::name).toList();
    }

    // -------------------------------------------------------------- groups

    /** A consumer group of this application and the topics it reads. */
    public record GroupSpec(String id, String purpose, List<String> topics) {
    }

    /**
     * The three independent consumers of the CDR stream. Different group ids
     * mean each group receives EVERY record (publish/subscribe); the members
     * inside one group split the partitions between them (queue). That is
     * Module 1's answer to "queue or topic?": Kafka gives you both at once.
     */
    public List<GroupSpec> groups() {
        return List.of(
                new GroupSpec(group("billing"), "Rates every CDR and publishes billing.charges", cdrTopics()),
                new GroupSpec(group("fraud-detection"), "Watches voice CDRs for international-call bursts", List.of(voice())),
                new GroupSpec(group("usage-analytics"), "Counts usage per type and per subscriber", cdrTopics()));
    }

    // -------------------------------------------------------------- designs

    /** The topic contracts, one per workload class. Read the "why" column out loud in class. */
    public List<TopicSpec> specs() {
        return List.of(
                new TopicSpec(voice(), "High-volume events",
                        "Voice call records. Every one becomes money on a bill.",
                        "min.insync.replicas=2 with acks=all: an acknowledged CDR survives a broker failure. "
                                + "7 days covers the longest billing-job outage. 256 MiB segments so retention has "
                                + "closed segments to delete.",
                        6, 3, config(
                                "cleanup.policy", "delete",
                                "retention.ms", DAYS_7,
                                "segment.bytes", "268435456",
                                "min.insync.replicas", "2")),

                new TopicSpec(sms(), "High-volume events",
                        "SMS records. Smaller volume than voice, same durability.",
                        "Shorter retention (3 days) because SMS volume is lower and re-rating windows are short.",
                        3, 3, config(
                                "cleanup.policy", "delete",
                                "retention.ms", DAYS_3,
                                "min.insync.replicas", "2")),

                new TopicSpec(data(), "High-volume events",
                        "Mobile data session records. The largest CDR stream.",
                        "retention.bytes is PER PARTITION: 5 GiB x 6 partitions caps the topic at 30 GiB. "
                                + "Whichever of 3 days or 5 GiB is hit first wins.",
                        6, 3, config(
                                "cleanup.policy", "delete",
                                "retention.ms", DAYS_3,
                                "retention.bytes", "5368709120",
                                "segment.bytes", "536870912",
                                "min.insync.replicas", "2")),

                planSpec(),

                new TopicSpec(charges(), "Financial",
                        "Rated charges that feed invoicing and the customer app.",
                        "Money: acks=all + min.insync.replicas=2, and 30 days so the invoicing system can replay "
                                + "a whole billing cycle.",
                        6, 3, config(
                                "cleanup.policy", "delete",
                                "retention.ms", DAYS_30,
                                "min.insync.replicas", "2")),

                telemetrySpec(),

                new TopicSpec(audit(), "Audit / compliance",
                        "Who changed what: plan changes, config changes, fraud alerts.",
                        "Regulator-driven 90 days, one partition to keep a single total order of events, "
                                + "min.insync.replicas=2.",
                        1, 3, config(
                                "cleanup.policy", "delete",
                                "retention.ms", DAYS_90,
                                "min.insync.replicas", "2")),

                new TopicSpec(drill(), "Durability drill",
                        "Scratch topic for the acks / min.insync.replicas experiment.",
                        "One partition makes the ISR easy to read. Short retention: it is a scratch topic.",
                        1, 3, config(
                                "cleanup.policy", "delete",
                                "retention.ms", "3600000",
                                "min.insync.replicas", "2")));
    }

    /**
     * Telemetry trades durability for speed and disk. On Confluent Cloud the replication factor is
     * fixed at 3 and compression.type is fixed at "producer" (the value we want anyway), so only
     * acks=1 and min.insync.replicas=1 remain of the trade-off.
     */
    private TopicSpec telemetrySpec() {
        if (confluentCloud) {
            return new TopicSpec(telemetry(), "Telemetry",
                    "Cell tower measurements, thousands per second.",
                    "Losing a few points is fine, so acks=1 and min.insync.replicas=1 buy speed. CONFLUENT CLOUD: "
                            + "RF is fixed at 3 and compression.type is fixed at 'producer' by Confluent, so the "
                            + "RF 2 saving of the self-managed design is not available. Retention is short in time AND size.",
                    3, 3, config(
                            "cleanup.policy", "delete",
                            "retention.ms", "3600000",
                            "retention.bytes", "1073741824",
                            "segment.bytes", "134217728",
                            "min.insync.replicas", "1"));
        }
        return new TopicSpec(telemetry(), "Telemetry",
                "Cell tower measurements, thousands per second.",
                "Losing a few points is fine, so acks=1, RF 2 and min.insync.replicas=1 buy speed and disk. "
                        + "Retention is short in time AND size. compression.type=producer means the broker "
                        + "stores the producer's lz4 batches as they are.",
                3, 2, config(
                        "cleanup.policy", "delete",
                        "retention.ms", "3600000",
                        "retention.bytes", "1073741824",
                        "segment.bytes", "134217728",
                        "compression.type", "producer",
                        "min.insync.replicas", "1"));
    }

    /**
     * subscriber.plan holds the CURRENT plan of every subscriber, so it is
     * compacted (Module 3 section 5.5): the latest record per MSISDN is kept
     * forever, older ones are cleaned up, and a null value deletes the subscriber.
     */
    private TopicSpec planSpec() {
        if (confluentCloud) {
            // Cloud: min.cleanable.dirty.ratio cannot be set, segment.ms >= 10 min and
            // max.compaction.lag.ms >= 6 h. Compaction still runs, but not within a minute.
            return new TopicSpec(plan(), "State / changelog",
                    "The current plan of every subscriber. A tombstone (null value) removes a subscriber.",
                    "Compacted: the latest plan per MSISDN is kept, old versions are cleaned. CONFLUENT CLOUD: "
                            + "segment.ms has a 10-minute minimum and the cleaner is Confluent's, so compaction "
                            + "takes hours, not a minute. Watch it on the local cluster.",
                    3, 3, config(
                            "cleanup.policy", "compact",
                            "min.insync.replicas", "2",
                            "segment.ms", labMode ? "600000" : "3600000",
                            "max.compaction.lag.ms", labMode ? "21600000" : "86400000",
                            "delete.retention.ms", "86400000"));
        }
        Map<String, String> configs = labMode
                ? config(
                        "cleanup.policy", "compact",
                        "min.insync.replicas", "2",
                        // LAB VALUES: make compaction visible within a minute.
                        "segment.ms", "30000",
                        "min.cleanable.dirty.ratio", "0.01",
                        "max.compaction.lag.ms", "60000",
                        "delete.retention.ms", "10000")
                : config(
                        "cleanup.policy", "compact",
                        "min.insync.replicas", "2",
                        "segment.ms", "3600000",
                        "min.cleanable.dirty.ratio", "0.2",
                        "max.compaction.lag.ms", "86400000",
                        "delete.retention.ms", "86400000");
        String why = labMode
                ? "LAB MODE: segment.ms=30s, dirty ratio 0.01 and max.compaction.lag.ms=60s make compaction "
                        + "happen in about a minute. Production uses hours (telco.lab-mode=false). Keys are mandatory."
                : "Compacted: the latest plan per MSISDN is kept, old versions are cleaned. Only closed segments "
                        + "are cleaned, so segment.ms bounds how long stale plans linger.";
        return new TopicSpec(plan(), "State / changelog",
                "The current plan of every subscriber. A tombstone (null value) removes a subscriber.",
                why, 3, 3, configs);
    }

    private static Map<String, String> config(String... keyValues) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }
}
