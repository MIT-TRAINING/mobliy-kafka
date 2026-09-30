package com.training.kafka.telco.ops;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.training.kafka.telco.common.AuditPublisher;
import com.training.kafka.telco.topics.TopicCatalog;
import com.training.kafka.telco.topics.TopicSpec;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Service;

/**
 * The operator's view of the platform, built only on the Kafka Admin API. Each
 * method notes the CLI command it replaces, so a class can run both and compare.
 * Nothing here needs a database: the cluster is the source of truth.
 */
@Service
public class OpsService {

    private static final long TIMEOUT_SECONDS = 15;

    /** Topic settings worth showing. The full list has 30+ entries and hides the ones that matter. */
    private static final List<String> SHOWN_CONFIGS = List.of(
            "cleanup.policy", "retention.ms", "retention.bytes", "segment.bytes", "segment.ms",
            "min.insync.replicas", "compression.type", "max.message.bytes",
            "min.cleanable.dirty.ratio", "max.compaction.lag.ms", "delete.retention.ms");

    /** Settings the dashboard may change. Anything else is refused. */
    private static final Set<String> CHANGEABLE_CONFIGS = Set.copyOf(SHOWN_CONFIGS);

    private final Admin admin;
    private final TopicCatalog catalog;
    private final AuditPublisher audit;

    public OpsService(Admin admin, TopicCatalog catalog, AuditPublisher audit) {
        this.admin = admin;
        this.catalog = catalog;
        this.audit = audit;
    }

    // ---------------------------------------------------------------- cluster

    /** kafka-metadata-quorum.sh describe --status, kafka-broker-api-versions.sh */
    public ClusterView cluster() throws Exception {
        DescribeClusterResult cluster = admin.describeCluster(new DescribeClusterOptions().includeFencedBrokers(true));
        QuorumInfo quorum = await(admin.describeMetadataQuorum().quorumInfo());
        List<BrokerView> brokers = await(cluster.nodes()).stream()
                .sorted(Comparator.comparingInt(Node::id))
                .map(n -> new BrokerView(n.id(), n.host() + ":" + n.port(), n.isFenced()))
                .toList();
        return new ClusterView(await(cluster.clusterId()), quorum.leaderId(), brokers);
    }

    // ----------------------------------------------------------------- topics

    /**
     * kafka-topics.sh --describe + kafka-configs.sh --describe for every topic of the platform,
     * including WHERE each value comes from (the "synonyms" of Module 3 section 2.2).
     */
    public List<TopicView> topics() throws Exception {
        List<TopicSpec> specs = catalog.specs();
        List<String> names = specs.stream().map(TopicSpec::name).toList();

        Map<String, TopicDescription> descriptions = await(admin.describeTopics(names).allTopicNames());
        // includeSynonyms is what makes the precedence chain visible (kafka-configs.sh --describe always asks for it)
        Map<ConfigResource, Config> configs = await(admin.describeConfigs(
                names.stream().map(n -> new ConfigResource(ConfigResource.Type.TOPIC, n)).toList(),
                new DescribeConfigsOptions().includeSynonyms(true)).all());

        List<TopicView> views = new ArrayList<>();
        for (TopicSpec spec : specs) {
            TopicDescription description = descriptions.get(spec.name());
            Config config = configs.get(new ConfigResource(ConfigResource.Type.TOPIC, spec.name()));
            views.add(toView(spec, description, config));
        }
        return views;
    }

    public TopicView topic(String name) throws Exception {
        return topics().stream().filter(t -> t.name().equals(name)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Not a topic of this platform: " + name));
    }

    /**
     * kafka-configs.sh --alter --entity-type topics --entity-name T --add-config KEY=VALUE
     * A dynamic change: no restart, no downtime, same value on every broker (Module 3 section 4.3).
     */
    public TopicView setConfig(String topic, String key, String value) throws Exception {
        requireChangeable(topic, key);
        String before = effectiveValue(topic, key);
        alter(topic, new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET));
        audit.publish("TOPIC_CONFIG_CHANGED", topic, key + ": " + before + " -> " + value);
        return topic(topic);
    }

    /** kafka-configs.sh --alter ... --delete-config KEY : the value falls back to the broker/default layer. */
    public TopicView resetConfig(String topic, String key) throws Exception {
        requireChangeable(topic, key);
        String before = effectiveValue(topic, key);
        alter(topic, new AlterConfigOp(new ConfigEntry(key, null), AlterConfigOp.OpType.DELETE));
        audit.publish("TOPIC_CONFIG_RESET", topic, key + ": override removed (was " + before + ")");
        return topic(topic);
    }

    // ---------------------------------------------------------------- storage

    /** kafka-log-dirs.sh --describe --topic-list ... : bytes on disk per topic, per broker. */
    public List<StorageView> storage() throws Exception {
        Set<String> ours = Set.copyOf(catalog.names());
        Map<String, Map<Integer, Long>> bytesByTopicAndBroker = new TreeMap<>();
        Map<String, Map<Integer, Long>> largestReplicaByPartition = new TreeMap<>();

        List<Integer> brokerIds = await(admin.describeCluster().nodes()).stream().map(Node::id).toList();
        Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> perBroker =
                admin.describeLogDirs(brokerIds).descriptions();

        for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirDescription>>> entry : perBroker.entrySet()) {
            Map<String, LogDirDescription> dirs;
            try {
                dirs = await(entry.getValue());
            } catch (ExecutionException e) {
                continue;               // a broker that is down simply has nothing to report
            }
            for (LogDirDescription dir : dirs.values()) {
                for (Map.Entry<TopicPartition, ReplicaInfo> replica : dir.replicaInfos().entrySet()) {
                    TopicPartition tp = replica.getKey();
                    if (!ours.contains(tp.topic())) {
                        continue;
                    }
                    long size = replica.getValue().size();
                    bytesByTopicAndBroker.computeIfAbsent(tp.topic(), t -> new TreeMap<>())
                            .merge(entry.getKey(), size, Long::sum);
                    largestReplicaByPartition.computeIfAbsent(tp.topic(), t -> new TreeMap<>())
                            .merge(tp.partition(), size, Math::max);
                }
            }
        }

        return catalog.specs().stream().map(spec -> {
            Map<Integer, Long> byBroker = bytesByTopicAndBroker.getOrDefault(spec.name(), Map.of());
            Map<Integer, Long> byPartition = largestReplicaByPartition.getOrDefault(spec.name(), Map.of());
            long total = byBroker.values().stream().mapToLong(Long::longValue).sum();
            return new StorageView(spec.name(), total, byBroker, byPartition);
        }).toList();
    }

    /**
     * Log start and end offset per partition. When retention deletes segments, the START offset moves
     * forward while the END offset only ever grows. The records still readable are the difference.
     * CLI: kafka-get-offsets.sh --time earliest / --time latest
     */
    public OffsetsView offsets(String topic) throws Exception {
        requireOurs(topic);
        TopicDescription description = await(admin.describeTopics(List.of(topic)).allTopicNames()).get(topic);
        List<TopicPartition> partitions = description.partitions().stream()
                .map(p -> new TopicPartition(topic, p.partition())).toList();
        Map<TopicPartition, ListOffsetsResultInfo> earliest = await(admin.listOffsets(specFor(partitions, OffsetSpec.earliest())).all());
        Map<TopicPartition, ListOffsetsResultInfo> latest = await(admin.listOffsets(specFor(partitions, OffsetSpec.latest())).all());

        List<PartitionOffsets> rows = partitions.stream().map(tp -> {
            long start = earliest.get(tp).offset();
            long end = latest.get(tp).offset();
            return new PartitionOffsets(tp.partition(), start, end, end - start);
        }).toList();
        return new OffsetsView(topic, rows.stream().mapToLong(PartitionOffsets::readableRecords).sum(), rows);
    }

    // ----------------------------------------------------------------- groups

    /** kafka-consumer-groups.sh --describe --group G  for each consumer group of the platform. */
    public List<GroupView> groups() throws Exception {
        List<GroupView> views = new ArrayList<>();
        for (TopicCatalog.GroupSpec spec : catalog.groups()) {
            views.add(group(spec));
        }
        return views;
    }

    private GroupView group(TopicCatalog.GroupSpec spec) throws Exception {
        ConsumerGroupDescription description = await(admin.describeConsumerGroups(List.of(spec.id())).all()).get(spec.id());
        Map<TopicPartition, OffsetAndMetadata> committed =
                await(admin.listConsumerGroupOffsets(spec.id()).partitionsToOffsetAndMetadata());

        Map<String, TopicDescription> topicDescriptions = await(admin.describeTopics(spec.topics()).allTopicNames());
        List<TopicPartition> partitions = topicDescriptions.values().stream()
                .flatMap(d -> d.partitions().stream().map(p -> new TopicPartition(d.name(), p.partition())))
                .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                .toList();
        Map<TopicPartition, ListOffsetsResultInfo> end = await(admin.listOffsets(specFor(partitions, OffsetSpec.latest())).all());
        Map<TopicPartition, ListOffsetsResultInfo> start = await(admin.listOffsets(specFor(partitions, OffsetSpec.earliest())).all());

        Map<String, Long> lagByTopic = new LinkedHashMap<>();
        long totalLag = 0;
        for (TopicPartition tp : partitions) {
            OffsetAndMetadata c = committed.get(tp);
            // Never committed: the group would start at the earliest retained record, so that is the lag
            long position = c != null ? c.offset() : start.get(tp).offset();
            long lag = Math.max(0, end.get(tp).offset() - position);
            lagByTopic.merge(tp.topic(), lag, Long::sum);
            totalLag += lag;
        }

        List<MemberView> members = description.members().stream()
                .sorted(Comparator.comparing(MemberDescription::clientId))
                .map(m -> new MemberView(m.clientId(),
                        m.assignment().topicPartitions().stream()
                                .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                                .map(tp -> tp.topic() + "-" + tp.partition()).toList()))
                .toList();

        return new GroupView(spec.id(), spec.purpose(), description.groupState().toString(), members, totalLag, lagByTopic);
    }

    // ---------------------------------------------------------------- helpers

    private TopicView toView(TopicSpec spec, TopicDescription description, Config config) {
        Map<String, ConfigEntry> byName = config.entries().stream()
                .collect(Collectors.toMap(ConfigEntry::name, e -> e, (a, b) -> a));

        List<ConfigView> shown = SHOWN_CONFIGS.stream()
                .filter(byName::containsKey)
                .map(name -> {
                    ConfigEntry e = byName.get(name);
                    List<String> synonyms = e.synonyms().stream()
                            .map(s -> s.source() + ":" + s.name() + "=" + s.value()).toList();
                    return new ConfigView(name, e.value(), e.source().toString(), spec.configs().get(name), synonyms);
                }).toList();

        int minIsr = Integer.parseInt(byName.get("min.insync.replicas").value());
        List<PartitionView> partitions = description.partitions().stream()
                .sorted(Comparator.comparingInt(TopicPartitionInfo::partition))
                .map(p -> new PartitionView(p.partition(),
                        p.leader() == null ? null : p.leader().id(),
                        ids(p.replicas()), ids(p.isr())))
                .toList();
        // Kafka caps the effective minimum at the number of replicas: min.insync.replicas=4 on an RF-3 topic
        // behaves like 3. So compare the ISR with the capped value, not the raw setting.
        boolean underMinIsr = partitions.stream().anyMatch(p -> p.isr().size() < Math.min(minIsr, p.replicas().size()));
        boolean underReplicated = partitions.stream().anyMatch(p -> p.isr().size() < p.replicas().size());

        return new TopicView(spec.name(), spec.workloadClass(), spec.purpose(), spec.whyConfigured(),
                spec.partitions(), spec.replicas(), minIsr, underMinIsr, underReplicated, shown, partitions);
    }

    private void alter(String topic, AlterConfigOp op) throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        await(admin.incrementalAlterConfigs(Map.of(resource, List.of(op))).all());
    }

    private String effectiveValue(String topic, String key) throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config config = await(admin.describeConfigs(List.of(resource)).all()).get(resource);
        ConfigEntry entry = config.get(key);
        return entry == null ? "(unset)" : entry.value();
    }

    private void requireChangeable(String topic, String key) {
        requireOurs(topic);
        if (!CHANGEABLE_CONFIGS.contains(key)) {
            throw new IllegalArgumentException("'" + key + "' cannot be changed here. Allowed: " + CHANGEABLE_CONFIGS
                    + ". Note topic-level names have no 'log.' prefix (Module 3 section 4.2).");
        }
    }

    private void requireOurs(String topic) {
        if (!catalog.names().contains(topic)) {
            throw new IllegalArgumentException("Not a topic of this platform: " + topic);
        }
    }

    private static Map<TopicPartition, OffsetSpec> specFor(Collection<TopicPartition> partitions, OffsetSpec spec) {
        Map<TopicPartition, OffsetSpec> map = new HashMap<>();
        partitions.forEach(tp -> map.put(tp, spec));
        return map;
    }

    private static List<Integer> ids(List<Node> nodes) {
        return nodes.stream().map(Node::id).toList();
    }

    static <T> T await(KafkaFuture<T> future) throws ExecutionException, InterruptedException, TimeoutException {
        return future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // ------------------------------------------------------------------ views

    public record ClusterView(String clusterId, int activeControllerId, List<BrokerView> brokers) {}

    public record BrokerView(int id, String address, boolean fenced) {}

    /**
     * @param minInsyncReplicas the EFFECTIVE value; underMinIsr means writes with acks=all are being rejected right now
     */
    public record TopicView(String name, String workloadClass, String purpose, String whyConfigured,
                            int partitionCount, int replicationFactor, int minInsyncReplicas,
                            boolean underMinIsr, boolean underReplicated,
                            List<ConfigView> configs, List<PartitionView> partitions) {}

    /**
     * @param source     which layer the effective value comes from (DYNAMIC_TOPIC_CONFIG, STATIC_BROKER_CONFIG, DEFAULT_CONFIG, ...)
     * @param declared   what TopicCatalog declares for this setting, or null if it declares nothing
     * @param synonyms   the whole precedence chain, highest priority first
     */
    public record ConfigView(String name, String value, String source, String declared, List<String> synonyms) {}

    public record PartitionView(int partition, Integer leader, List<Integer> replicas, List<Integer> isr) {}

    public record StorageView(String topic, long totalBytesAllReplicas, Map<Integer, Long> bytesByBroker,
                              Map<Integer, Long> bytesByPartition) {}

    public record OffsetsView(String topic, long readableRecords, List<PartitionOffsets> partitions) {}

    public record PartitionOffsets(int partition, long logStartOffset, long logEndOffset, long readableRecords) {}

    public record GroupView(String groupId, String purpose, String state, List<MemberView> members, long totalLag,
                            Map<String, Long> lagByTopic) {}

    public record MemberView(String clientId, List<String> partitions) {}
}
