package com.examples.kafka;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
 * Exactly-once Kafka → Kafka (guide §7.4): read CDRs, write rated CDRs, and
 * commit the input offsets in the SAME transaction.
 *
 *   rate <in-topic> <out-topic> <group> [--abort-every=N]
 *
 * transactional.id = <group>-tx, so it falls under your lNN. prefix ACL.
 * Starts at the END of the input topic: only CDRs produced while it runs are rated.
 * --abort-every=N aborts every Nth transaction and rewinds, so the same CDRs
 * are rated again in the next transaction: read_uncommitted readers see the
 * aborted copies, read_committed readers do not.
 */
public final class TransactionalRater {

    private static final Logger log = LoggerFactory.getLogger(TransactionalRater.class);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private TransactionalRater() { }

    public static void run(List<String> args) throws Exception {
        String in = args.get(0);                             // e.g. l07.cdr.voice
        String out = args.get(1);                            // e.g. l07.cdr.rated
        String group = args.get(2);                          // e.g. l07.rating
        int abortEvery = args.size() > 3 && args.get(3).startsWith("--abort-every=")
                ? Integer.parseInt(args.get(3).substring(14)) : 0;
        if (abortEvery == 1) throw new IllegalArgumentException("--abort-every must be 0 or >= 2");

        Properties base = ClientConfig.baseConfig();

        Properties cp = new Properties();
        cp.putAll(base);
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");    // offsets go into the transaction
        cp.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        cp.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");          // small transactions, easy to follow

        Properties pp = new Properties();
        pp.putAll(base);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pp.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, group + "-tx");   // implies acks=all + idempotence

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp);
        KafkaProducer<String, String> producer = new KafkaProducer<>(pp);
        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try { main.join(); } catch (InterruptedException ignored) { }
        }));

        try {
            producer.initTransactions();                     // fences older instances with the same id
            log.info("Rating {} -> {} with transactional.id={}-tx, abort every {}",
                    in, out, group, abortEvery == 0 ? "never" : abortEvery + " transactions");
            consumer.subscribe(List.of(in));
            int txn = 0;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) continue;

                txn++;
                producer.beginTransaction();
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                Map<TopicPartition, Long> firstOffsets = new HashMap<>();
                for (ConsumerRecord<String, String> r : records) {
                    producer.send(new ProducerRecord<>(out, r.key(), rate(r.value())));
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    firstOffsets.putIfAbsent(tp, r.offset());
                    offsets.put(tp, new OffsetAndMetadata(r.offset() + 1));
                }
                producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());

                if (abortEvery > 0 && txn % abortEvery == 0) {
                    producer.abortTransaction();
                    firstOffsets.forEach(consumer::seek);    // rewind: rate these CDRs again
                    log.warn("Transaction {} ABORTED  ({} rated CDRs discarded, input rewound)", txn, records.count());
                } else {
                    producer.commitTransaction();
                    log.info("Transaction {} committed ({} rated CDRs + input offsets)", txn, records.count());
                }
            }
        } catch (WakeupException e) {
            // expected on shutdown
        } finally {
            consumer.close();
            producer.close();
        }
    }

    /** Adds a charge: 0.25 per started minute. Non-JSON values are passed through as unrated. */
    private static String rate(String value) {
        try {
            JsonNode cdr = JSON.readTree(value);
            long seconds = cdr.path("durationSec").asLong();
            double charge = Math.ceil(seconds / 60.0) * 0.25;
            return "{\"cdrId\":\"" + cdr.path("cdrId").asString() + "\",\"msisdn\":\"" + cdr.path("msisdn").asString()
                    + "\",\"durationSec\":" + seconds + ",\"charge\":" + charge + "}";
        } catch (Exception e) {
            return "{\"unrated\":true}";
        }
    }
}
