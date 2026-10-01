package com.examples.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A durable CDR producer (guide §8.3).
 *
 *   produce <topic> <count> [config=value ...]
 *
 * Sends <count> JSON CDRs keyed by 20 MSISDNs (966500000000 ... 966500000019).
 * Trailing config=value arguments override the defaults below, so the same
 * program can be measured with different tuning (Lab 01 Part 5).
 */
public final class CdrProducer {

    private static final Logger log = LoggerFactory.getLogger(CdrProducer.class);

    /** Producer metrics printed after the run: what an administrator reads when tuning. */
    private static final List<String> METRICS = List.of(
            "batch-size-avg", "records-per-request-avg", "request-total",
            "compression-rate-avg", "record-retry-total", "record-error-total",
            "produce-throttle-time-avg");

    private CdrProducer() { }

    public static void run(List<String> args) throws Exception {
        String topic = args.get(0);                          // e.g. l07.cdr.voice
        int count = Integer.parseInt(args.get(1));

        Properties props = ClientConfig.baseConfig();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // The production baseline (guide §3.4): durable, duplicate-free, batched, compressed
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "cdr-producer");
        ClientConfig.applyOverrides(props, args.subList(2, args.size()));

        log.info("Producer settings: acks={} enable.idempotence={} linger.ms={} batch.size={} compression.type={}",
                props.get(ProducerConfig.ACKS_CONFIG), props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG),
                props.get(ProducerConfig.LINGER_MS_CONFIG), props.get(ProducerConfig.BATCH_SIZE_CONFIG),
                props.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));

        AtomicInteger failed = new AtomicInteger();
        Map<Integer, Integer> perPartition = new TreeMap<>();
        long start = System.nanoTime();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                String msisdn = String.format("9665000000%02d", i % 20);
                String cdr = "{\"cdrId\":\"" + UUID.randomUUID().toString().substring(0, 8) + "\",\"msisdn\":\"" + msisdn
                        + "\",\"type\":\"voice\",\"durationSec\":" + (30 + i % 300) + "}";
                boolean show = i < 5;                        // print the first few, summarise the rest
                producer.send(new ProducerRecord<>(topic, msisdn, cdr), (md, ex) -> {
                    if (ex != null) {
                        failed.incrementAndGet();
                        log.error("Delivery failed for key={}: {}", msisdn, ex.toString());   // alert, DLT or stop
                        return;
                    }
                    synchronized (perPartition) {
                        perPartition.merge(md.partition(), 1, Integer::sum);
                    }
                    if (show) {
                        log.info("key={} -> partition={} offset={}", msisdn, md.partition(), md.offset());
                    }
                });
            }
            producer.flush();                                // drain the accumulator before close

            long ms = Math.max(1, (System.nanoTime() - start) / 1_000_000);
            log.info("Sent {} records in {} ms ({} records/sec), {} failed",
                    count - failed.get(), ms, (count - failed.get()) * 1000L / ms, failed.get());
            log.info("Records per partition: {}", perPartition);
            printMetrics(producer.metrics());
        }
    }

    private static void printMetrics(Map<MetricName, ? extends Metric> metrics) {
        Map<String, Object> wanted = new TreeMap<>((a, b) -> METRICS.indexOf(a) - METRICS.indexOf(b));
        metrics.forEach((name, metric) -> {
            if (name.group().equals("producer-metrics") && METRICS.contains(name.name())) {
                wanted.put(name.name(), metric.metricValue());
            }
        });
        wanted.forEach((name, value) -> log.info(String.format("  %-26s %10.1f", name, (Double) value)));
    }
}
