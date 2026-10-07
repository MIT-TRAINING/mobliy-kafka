package com.training.kafka.telco.ops;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.training.kafka.telco.common.KafkaConnection;
import com.training.kafka.telco.topics.TopicCatalog;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

/**
 * Reads a whole topic from the beginning and reports what is really in it.
 *
 * For a compacted topic this shows the effect of the log cleaner: after
 * compaction the same keys exist but far fewer records. It is exactly the work
 * a new service instance does at start-up to rebuild state.
 *
 * CLI: kafka-console-consumer.sh --topic T --from-beginning --timeout-ms 5000 | wc -l
 */
@Service
public class ReplayReader {

    private static final long MAX_MILLIS = 20_000;

    private final KafkaConnection connection;
    private final TopicCatalog catalog;

    public ReplayReader(KafkaConnection connection, TopicCatalog catalog) {
        this.connection = connection;
        this.catalog = catalog;
    }

    public ReplayStats replay(String name) {
        String topic = catalog.resolve(name);
        if (!catalog.names().contains(topic)) {
            throw new IllegalArgumentException("Not a topic of this platform: " + topic);
        }
        long started = System.currentTimeMillis();
        try (KafkaConsumer<String, String> consumer = connection.newReader()) {
            List<TopicPartition> partitions = new ArrayList<>();
            for (PartitionInfo info : consumer.partitionsFor(topic)) {
                partitions.add(new TopicPartition(topic, info.partition()));
            }
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);

            long records = 0;
            long tombstones = 0;
            Map<String, Boolean> lastRecordIsTombstone = new LinkedHashMap<>();
            Map<Integer, Long> recordsByPartition = new HashMap<>();

            while (!reachedEnd(consumer, partitions, end) && System.currentTimeMillis() - started < MAX_MILLIS) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    records++;
                    recordsByPartition.merge(r.partition(), 1L, Long::sum);
                    boolean tombstone = r.value() == null;
                    if (tombstone) {
                        tombstones++;
                    }
                    lastRecordIsTombstone.put(r.key(), tombstone);
                }
            }
            long live = lastRecordIsTombstone.values().stream().filter(t -> !t).count();
            return new ReplayStats(topic, records, lastRecordIsTombstone.size(), live, tombstones,
                    new java.util.TreeMap<>(recordsByPartition), System.currentTimeMillis() - started);
        }
    }

    private static boolean reachedEnd(KafkaConsumer<String, String> consumer, List<TopicPartition> partitions,
                                      Map<TopicPartition, Long> end) {
        return partitions.stream().allMatch(tp -> consumer.position(tp) >= end.get(tp));
    }

    /**
     * @param recordsRead   records physically present in the topic
     * @param distinctKeys  different keys seen
     * @param liveKeys      keys whose latest record is NOT a tombstone (the "table" a new service rebuilds)
     * @param tombstones    null-value records still stored
     */
    public record ReplayStats(String topic, long recordsRead, long distinctKeys, long liveKeys, long tombstones,
                              Map<Integer, Long> recordsByPartition, long elapsedMillis) {}
}
