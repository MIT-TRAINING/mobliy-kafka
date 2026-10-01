package com.examples.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * A billing consumer with graceful shutdown (guide §8.4) and switchable
 * commit strategy and error handling for the Module 4 labs.
 *
 *   consume <topic> <group> [options] [config=value ...]
 *
 *   --commit=after     process, then commit (at-least-once, default)
 *   --commit=before    commit, then process (at-most-once)
 *   --commit=auto      enable.auto.commit=true, the client commits in the background
 *   --process-ms=N     simulate N ms of work (a billing-DB write) per record
 *   --json             parse each value as a CDR; a bad record is retried in place, forever
 *   --dlt=<topic>      with --json: send bad records to <topic> and move on
 *
 * Trailing config=value arguments are consumer configs, e.g. group.protocol=consumer.
 */
public final class CdrConsumer {

    private static final Logger log = LoggerFactory.getLogger(CdrConsumer.class);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private enum Commit { AFTER, BEFORE, AUTO }

    private enum Outcome { BILLED, DEAD_LETTERED, RETRY }

    private CdrConsumer() { }

    public static void run(List<String> args) throws Exception {
        String topic = args.get(0);                          // e.g. l07.cdr.voice
        String group = args.get(1);                          // e.g. l07.billing

        Commit commit = Commit.AFTER;
        long processMs = 0;
        boolean json = false;
        String dlt = null;
        List<String> overrides = new ArrayList<>();
        for (String a : args.subList(2, args.size())) {
            if (a.startsWith("--commit=")) commit = Commit.valueOf(a.substring(9).toUpperCase());
            else if (a.startsWith("--process-ms=")) processMs = Long.parseLong(a.substring(13));
            else if (a.equals("--json")) json = true;
            else if (a.startsWith("--dlt=")) { dlt = a.substring(6); json = true; }
            else overrides.add(a);
        }

        Properties base = ClientConfig.baseConfig();
        Properties props = new Properties();
        props.putAll(base);
        long pid = ProcessHandle.current().pid();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "cdr-consumer-" + pid);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(commit == Commit.AUTO));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");
        ClientConfig.applyOverrides(props, overrides);

        log.info("Started pid={} client.id={} group={} group.protocol={} commit={} process-ms={}{}",
                pid, props.get(ConsumerConfig.CLIENT_ID_CONFIG), group,
                props.getOrDefault(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic"),
                commit.name().toLowerCase(), processMs,
                json ? " json=on dlt=" + (dlt == null ? "none (retry forever)" : dlt) : "");

        KafkaProducer<String, String> dltProducer = dlt == null ? null : dltProducer(base);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();                               // makes poll() throw WakeupException
            try { main.join(); } catch (InterruptedException ignored) { }
        }));

        final Commit mode = commit;
        try {
            consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> parts) {
                    log.info("Revoked  {}", sorted(parts));
                    if (mode != Commit.AUTO && !parts.isEmpty()) {
                        consumer.commitSync();               // don't lose finished work
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> parts) {
                    log.info("Assigned {}", sorted(parts));
                    if (parts.isEmpty()) return;
                    Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new java.util.HashSet<>(parts));
                    for (TopicPartition tp : sorted(parts)) {
                        OffsetAndMetadata om = committed.get(tp);
                        log.info("  {} starts at {}", tp,
                                om == null ? "auto.offset.reset (no committed offset)" : "committed offset " + om.offset());
                    }
                }

                @Override
                public void onPartitionsLost(Collection<TopicPartition> parts) {
                    log.warn("Lost     {} (not committing: another member owns them now)", sorted(parts));
                }
            });

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) continue;
                if (mode == Commit.BEFORE) {
                    consumer.commitSync();                   // at-most-once: commit first, process later
                }
                Map<TopicPartition, OffsetAndMetadata> done = new HashMap<>();
                for (TopicPartition tp : records.partitions()) {
                    for (ConsumerRecord<String, String> r : records.records(tp)) {
                        if (processMs > 0) Thread.sleep(processMs);
                        Outcome outcome = json ? process(r, dlt, dltProducer) : Outcome.BILLED;
                        if (outcome == Outcome.RETRY) {
                            consumer.seek(tp, r.offset());   // blocking retry: same record next poll
                            Thread.sleep(1000);
                            break;                           // leave the rest of this partition for later
                        }
                        if (outcome == Outcome.BILLED) {
                            log.info("p={} off={} key={} value={}", r.partition(), r.offset(), r.key(), r.value());
                        }
                        done.put(tp, new OffsetAndMetadata(r.offset() + 1));
                    }
                }
                if (mode == Commit.AFTER && !done.isEmpty()) {
                    consumer.commitAsync(done, null);        // fast path; sync on revoke and on close
                }
            }
        } catch (WakeupException e) {
            // expected on shutdown
        } finally {
            try {
                if (mode != Commit.AUTO) {
                    consumer.commitSync();                   // final, reliable commit
                }
                log.info("Committed final offsets, closing (leaves the group)");
            } finally {
                consumer.close();                            // LeaveGroup → fast rebalance
                if (dltProducer != null) dltProducer.close();
            }
        }
    }

    /** BILLED or DEAD_LETTERED: the record is done and its offset may be committed. RETRY: it is not. */
    private static Outcome process(ConsumerRecord<String, String> r, String dlt,
                                   KafkaProducer<String, String> dltProducer) throws Exception {
        try {
            JsonNode cdr = JSON.readTree(r.value());
            if (!cdr.hasNonNull("cdrId") || !cdr.hasNonNull("msisdn")) {
                throw new IllegalArgumentException("missing cdrId/msisdn");
            }
            return Outcome.BILLED;                           // billing-DB write would go here
        } catch (Exception e) {
            String reason = e.getClass().getSimpleName() + ": " + firstLine(e.getMessage());
            if (dlt == null) {
                log.error("Cannot process p={} off={} ({}) - retrying in 1 s", r.partition(), r.offset(), reason);
                return Outcome.RETRY;
            }
            ProducerRecord<String, String> dead = new ProducerRecord<>(dlt, r.partition(), r.key(), r.value());
            dead.headers()
                .add("dlt.original.topic", r.topic().getBytes(StandardCharsets.UTF_8))
                .add("dlt.original.partition", String.valueOf(r.partition()).getBytes(StandardCharsets.UTF_8))
                .add("dlt.original.offset", String.valueOf(r.offset()).getBytes(StandardCharsets.UTF_8))
                .add("dlt.exception", reason.getBytes(StandardCharsets.UTF_8));
            dltProducer.send(dead).get();                    // durable in the DLT before we commit past it
            log.warn("Poison pill p={} off={} -> {} ({})", r.partition(), r.offset(), dlt, reason);
            return Outcome.DEAD_LETTERED;
        }
    }

    private static KafkaProducer<String, String> dltProducer(Properties base) {
        Properties p = new Properties();
        p.putAll(base);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "cdr-dlt-producer");
        return new KafkaProducer<>(p);
    }

    private static List<TopicPartition> sorted(Collection<TopicPartition> parts) {
        List<TopicPartition> list = new ArrayList<>(parts);
        list.sort(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));
        return list;
    }

    private static String firstLine(String s) {
        if (s == null) return "";
        int nl = s.indexOf('\n');
        return nl < 0 ? s : s.substring(0, nl);
    }
}
