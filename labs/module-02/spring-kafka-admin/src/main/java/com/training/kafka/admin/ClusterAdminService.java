package com.training.kafka.admin;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Service;

/**
 * Every method wraps one Kafka Admin API call. The Javadoc names the CLI
 * command that does the same thing, so you can run both and compare.
 *
 * Admin API calls are asynchronous and return KafkaFuture objects; this lab
 * simply waits for each result with a timeout.
 */
@Service
public class ClusterAdminService {

    private static final long TIMEOUT_SECONDS = 15;

    private final Admin admin;

    public ClusterAdminService(Admin admin) {
        this.admin = admin;
    }

    // ---------------------------------------------------------------- cluster

    /**
     * kafka-cluster.sh cluster-id, kafka-broker-api-versions.sh (broker list)
     * and kafka-metadata-quorum.sh describe --status.
     */
    public ClusterView describeCluster() throws Exception {
        // includeFencedBrokers: also list registered brokers that are down (fenced)
        DescribeClusterResult cluster = admin.describeCluster(new DescribeClusterOptions().includeFencedBrokers(true));
        QuorumInfo quorum = await(admin.describeMetadataQuorum().quorumInfo());

        List<BrokerView> brokers = await(cluster.nodes()).stream()
                .sorted(Comparator.comparingInt(Node::id))
                .map(n -> new BrokerView(n.id(), n.host(), n.port(), n.isFenced()))
                .toList();

        return new ClusterView(
                await(cluster.clusterId()),
                quorum.leaderId(),
                quorum.voters().stream().map(QuorumInfo.ReplicaState::replicaId).sorted().toList(),
                brokers);
    }

    // ----------------------------------------------------------------- topics

    /** kafka-topics.sh --list  (add --exclude-internal to hide __ topics) */
    public List<String> listTopics(boolean includeInternal) throws Exception {
        return await(admin.listTopics(new ListTopicsOptions().listInternal(includeInternal)).names())
                .stream().sorted().toList();
    }

    /** kafka-topics.sh --describe --topic NAME  plus  kafka-configs.sh --describe --topic NAME */
    public TopicView describeTopic(String name) throws Exception {
        TopicDescription description = await(admin.describeTopics(List.of(name)).allTopicNames()).get(name);

        List<PartitionView> partitions = description.partitions().stream()
                .map(p -> new PartitionView(
                        p.partition(),
                        p.leader() == null ? null : p.leader().id(), // null leader = partition offline
                        ids(p.replicas()),
                        ids(p.isr())))
                .toList();

        // Only configs that differ from the built-in defaults, like kafka-configs.sh --describe
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        Config config = await(admin.describeConfigs(List.of(resource)).all()).get(resource);
        Map<String, String> overrides = config.entries().stream()
                .filter(e -> e.source() != ConfigEntry.ConfigSource.DEFAULT_CONFIG)
                .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value, (a, b) -> a, java.util.TreeMap::new));

        return new TopicView(description.name(), description.topicId().toString(),
                description.isInternal(), partitions, overrides);
    }

    /** kafka-topics.sh --create --topic NAME --partitions P --replication-factor R --config k=v */
    public TopicView createTopic(CreateTopicRequest request) throws Exception {
        NewTopic topic = new NewTopic(request.name(),
                Optional.ofNullable(request.partitions()),                              // empty = broker default
                Optional.ofNullable(request.replicationFactor()).map(Integer::shortValue));
        if (request.configs() != null) {
            topic.configs(request.configs());
        }
        await(admin.createTopics(List.of(topic)).all());
        return describeTopic(request.name());
    }

    /** kafka-topics.sh --alter --topic NAME --partitions TOTAL  (can only increase) */
    public TopicView increasePartitions(String name, int totalCount) throws Exception {
        await(admin.createPartitions(Map.of(name, NewPartitions.increaseTo(totalCount))).all());
        return describeTopic(name);
    }

    /** kafka-topics.sh --delete --topic NAME */
    public void deleteTopic(String name) throws Exception {
        await(admin.deleteTopics(List.of(name)).all());
    }

    // --------------------------------------------------------- consumer groups

    /** kafka-consumer-groups.sh --describe --group GROUP */
    public GroupLagView groupLag(String groupId) throws Exception {
        ConsumerGroupDescription group = await(admin.describeConsumerGroups(List.of(groupId)).all()).get(groupId);

        // CURRENT-OFFSET: what the group has committed
        Map<TopicPartition, OffsetAndMetadata> committed =
                await(admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata());

        // LOG-END-OFFSET: the latest offset of each of those partitions
        Map<TopicPartition, OffsetSpec> latestSpec = committed.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResultInfo> logEnd = await(admin.listOffsets(latestSpec).all());

        List<PartitionLagView> partitions = committed.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .sorted(Comparator.comparing((Map.Entry<TopicPartition, OffsetAndMetadata> e) -> e.getKey().topic())
                        .thenComparingInt(e -> e.getKey().partition()))
                .map(e -> {
                    long current = e.getValue().offset();
                    long end = logEnd.get(e.getKey()).offset();
                    return new PartitionLagView(e.getKey().topic(), e.getKey().partition(), current, end, end - current);
                })
                .toList();

        long totalLag = partitions.stream().mapToLong(PartitionLagView::lag).sum();
        return new GroupLagView(groupId, group.groupState().toString(), group.members().size(), totalLag, partitions);
    }

    // ---------------------------------------------------------------- helpers

    private static <T> T await(org.apache.kafka.common.KafkaFuture<T> future)
            throws ExecutionException, InterruptedException, TimeoutException {
        return future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static List<Integer> ids(Collection<Node> nodes) {
        return nodes.stream().map(Node::id).toList();
    }

    // ------------------------------------------------------------------ views

    public record ClusterView(String clusterId, int activeControllerId, List<Integer> controllerVoters,
                              List<BrokerView> brokers) {}

    public record BrokerView(int id, String host, int port, boolean fenced) {}

    public record TopicView(String name, String topicId, boolean internal, List<PartitionView> partitions,
                            Map<String, String> configs) {}

    public record PartitionView(int partition, Integer leader, List<Integer> replicas, List<Integer> isr) {}

    public record CreateTopicRequest(String name, Integer partitions, Integer replicationFactor,
                                     Map<String, String> configs) {}

    public record GroupLagView(String groupId, String state, int members, long totalLag,
                               List<PartitionLagView> partitions) {}

    public record PartitionLagView(String topic, int partition, long currentOffset, long logEndOffset, long lag) {}
}
