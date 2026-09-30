package com.training.kafka.telco.ops;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.training.kafka.telco.common.KafkaConnection;
import com.training.kafka.telco.topics.TopicCatalog;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Service;

/**
 * Module 3 section 6, as a live experiment on the topic drill.durability
 * (RF 3, min.insync.replicas 2).
 *
 *   write(acks)  one record with the producer contract you choose: all, 1 or 0
 *   state()      what a consumer can actually SEE, and where the high watermark is
 *
 * Retries are switched off in these producers so the broker's answer is visible
 * immediately. A real producer retries a retriable error such as
 * NOT_ENOUGH_REPLICAS until delivery.timeout.ms (Module 4).
 */
@Service
public class DurabilityDrill {

    private final ProducerFactory<String, String> producerFactory;
    private final KafkaConnection connection;
    private final TopicCatalog catalog;
    private final OpsService ops;
    private final Map<String, KafkaTemplate<String, String>> templatesByAcks = new ConcurrentHashMap<>();

    public DurabilityDrill(ProducerFactory<String, String> producerFactory, KafkaConnection connection,
                           TopicCatalog catalog, OpsService ops) {
        this.producerFactory = producerFactory;
        this.connection = connection;
        this.catalog = catalog;
        this.ops = ops;
    }

    public WriteResult write(String acks, String payload) {
        if (!List.of("all", "1", "0").contains(acks)) {
            throw new IllegalArgumentException("acks must be all, 1 or 0");
        }
        KafkaTemplate<String, String> template = templatesByAcks.computeIfAbsent(acks, this::templateFor);
        long started = System.currentTimeMillis();
        try {
            RecordMetadata meta = template.send(catalog.drill(), "drill", payload)
                    .get(10, TimeUnit.SECONDS).getRecordMetadata();
            return new WriteResult(acks, true, meta.partition(), meta.offset(), null, null,
                    System.currentTimeMillis() - started);
        } catch (ExecutionException e) {
            Throwable cause = brokerError(e);
            return new WriteResult(acks, false, null, null, cause.getClass().getSimpleName(), cause.getMessage(),
                    System.currentTimeMillis() - started);
        } catch (TimeoutException | InterruptedException e) {
            return new WriteResult(acks, false, null, null, e.getClass().getSimpleName(),
                    "No answer from the cluster in 10 s", System.currentTimeMillis() - started);
        }
    }

    /** What the broker holds and what a consumer can read. */
    public DrillState state() throws Exception {
        OpsService.TopicView topic = ops.topic(catalog.drill());
        OpsService.PartitionView partition = topic.partitions().get(0);
        TopicPartition tp = new TopicPartition(catalog.drill(), 0);

        List<String> visible = new ArrayList<>();
        long highWatermark;
        try (KafkaConsumer<String, String> consumer = connection.newReader()) {
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            // For a consumer, the end offset IS the high watermark: the last offset replicated to enough replicas
            highWatermark = consumer.endOffsets(List.of(tp)).get(tp);
            long deadline = System.currentTimeMillis() + 6_000;
            while (consumer.position(tp) < highWatermark && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    visible.add(r.offset() + ": " + r.value());
                }
            }
        }
        int from = Math.max(0, visible.size() - 10);
        return new DrillState(topic.minInsyncReplicas(), partition.leader(), partition.replicas(), partition.isr(),
                topic.underMinIsr(), highWatermark, visible.size(), visible.subList(from, visible.size()));
    }

    /**
     * Spring wraps the broker's answer twice (ExecutionException, KafkaProducerException).
     * The interesting part is the Kafka error inside, e.g. NotEnoughReplicasException.
     */
    private static Throwable brokerError(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof org.apache.kafka.common.errors.ApiException) {
                return t;
            }
        }
        return error.getCause() != null ? error.getCause() : error;
    }

    private KafkaTemplate<String, String> templateFor(String acks) {
        return new KafkaTemplate<>(producerFactory.copyWithConfigurationOverride(Map.of(
                "acks", acks,
                "enable.idempotence", "false",
                "retries", "0",
                "linger.ms", "0",
                "request.timeout.ms", "4000",
                "delivery.timeout.ms", "5000",
                "max.block.ms", "5000")));
    }

    public record WriteResult(String acks, boolean acknowledged, Integer partition, Long offset, String error,
                              String message, long elapsedMillis) {}

    /**
     * @param highWatermark   the first offset a consumer can NOT read yet
     * @param visibleRecords  records a consumer really received
     */
    public record DrillState(int minInsyncReplicas, Integer leader, List<Integer> replicas, List<Integer> isr,
                             boolean belowMinInsync, long highWatermark, int visibleRecords, List<String> lastVisible) {}
}
