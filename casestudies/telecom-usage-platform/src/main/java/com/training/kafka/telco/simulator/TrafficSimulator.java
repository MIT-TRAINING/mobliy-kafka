package com.training.kafka.telco.simulator;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.training.kafka.telco.common.JsonCodec;
import com.training.kafka.telco.model.Cdr;
import com.training.kafka.telco.model.CdrType;
import com.training.kafka.telco.model.TelemetryPoint;
import com.training.kafka.telco.topics.TopicCatalog;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Plays the role of the mobile network: mediation systems and cell towers
 * that produce usage records and measurements.
 *
 * Every CDR is sent with the MSISDN as key (Module 1: keys and partitions).
 * Telemetry is sent with the cell id as key, through the cheaper producer.
 */
@Service
public class TrafficSimulator {

    private static final Logger log = LoggerFactory.getLogger(TrafficSimulator.class);
    private static final List<String> FOREIGN = List.of("AE", "EG", "PK", "IN", "GB", "US");
    private static final int CELLS = 40;
    private static final long TICK_MS = 100;

    private final KafkaTemplate<String, String> template;
    private final KafkaTemplate<String, String> telemetryTemplate;
    private final JsonCodec json;
    private final TopicCatalog topics;
    private final Subscribers subscribers;
    private final double internationalShare;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "traffic-simulator");
        t.setDaemon(true);
        return t;
    });
    private volatile ScheduledFuture<?> running;
    private volatile int ratePerSecond;
    private final AtomicLong generated = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    public TrafficSimulator(KafkaTemplate<String, String> template,
                            @Qualifier("telemetryTemplate") KafkaTemplate<String, String> telemetryTemplate,
                            JsonCodec json, TopicCatalog topics, Subscribers subscribers,
                            @Value("${telco.simulator.international-share:0.12}") double internationalShare) {
        this.template = template;
        this.telemetryTemplate = telemetryTemplate;
        this.json = json;
        this.topics = topics;
        this.subscribers = subscribers;
        this.internationalShare = internationalShare;
    }

    // ------------------------------------------------------------------ CDRs

    /** Send COUNT random CDRs and report which partition each one landed in. */
    public Map<String, Integer> generateCdrs(int count) {
        Map<String, Integer> perPartition = new TreeMap<>();
        CompletableFuture<?>[] sends = new CompletableFuture<?>[count];
        for (int i = 0; i < count; i++) {
            sends[i] = sendCdr(randomCdr(), perPartition);
        }
        CompletableFuture.allOf(sends).join();
        return perPartition;
    }

    /**
     * One subscriber makes CALLS international calls, one per second of event time,
     * for the fraud rule to catch. Sent in order with the same key, so they are
     * read in order.
     */
    public int fraudBurst(String msisdn, int calls) {
        CompletableFuture<?>[] sends = new CompletableFuture<?>[calls];
        Instant start = Instant.now();
        for (int i = 0; i < calls; i++) {
            String country = FOREIGN.get(i % FOREIGN.size());
            Cdr cdr = new Cdr(UUID.randomUUID().toString(), CdrType.VOICE, msisdn, "00" + country + i, country,
                    30 + i, 0, start.plusSeconds(i), cellId());
            sends[i] = sendCdr(cdr, null);
        }
        CompletableFuture.allOf(sends).join();
        return calls;
    }

    // ------------------------------------------------------------ continuous

    public synchronized void start(int recordsPerSecond) {
        stop();
        this.ratePerSecond = Math.max(1, recordsPerSecond);
        int perTick = Math.max(1, ratePerSecond * (int) TICK_MS / 1000);
        running = scheduler.scheduleAtFixedRate(() -> {
            try {
                for (int i = 0; i < perTick; i++) {
                    sendCdr(randomCdr(), null);
                }
            } catch (RuntimeException e) {
                log.warn("Simulator tick failed: {}", e.toString());
            }
        }, 0, TICK_MS, TimeUnit.MILLISECONDS);
        log.info("Traffic simulator started: about {} CDRs/second", ratePerSecond);
    }

    public synchronized void stop() {
        if (running != null) {
            running.cancel(false);
            running = null;
            log.info("Traffic simulator stopped");
        }
    }

    public Status status() {
        return new Status(running != null, running != null ? ratePerSecond : 0, generated.get(), failed.get());
    }

    @PreDestroy
    void shutdown() {
        scheduler.shutdownNow();
    }

    // ------------------------------------------------------------- telemetry

    /**
     * COUNT radio measurements for the telemetry topic. Uses the acks=1 / lz4 producer,
     * because losing a data point does not cost money.
     */
    public int generateTelemetry(int count) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        CompletableFuture<?>[] sends = new CompletableFuture<?>[count];
        for (int i = 0; i < count; i++) {
            String cell = cellId();
            TelemetryPoint point = new TelemetryPoint(cell, Instant.now(), random.nextInt(0, 400),
                    Math.round(random.nextDouble(5, 98) * 10) / 10.0, Math.round(random.nextDouble(2, 150) * 10) / 10.0,
                    random.nextInt(0, 30));
            sends[i] = telemetryTemplate.send(topics.telemetry(), cell, json.toJson(point));
        }
        CompletableFuture.allOf(sends).join();
        return count;
    }

    // --------------------------------------------------------------- helpers

    private CompletableFuture<?> sendCdr(Cdr cdr, Map<String, Integer> perPartition) {
        generated.incrementAndGet();
        return template.send(topics.topicFor(cdr.type()), cdr.msisdn(), json.toJson(cdr))
                .whenComplete((result, error) -> {
                    if (error != null) {
                        failed.incrementAndGet();
                    } else if (perPartition != null) {
                        String where = result.getRecordMetadata().topic() + "-" + result.getRecordMetadata().partition();
                        synchronized (perPartition) {
                            perPartition.merge(where, 1, Integer::sum);
                        }
                    }
                });
    }

    private Cdr randomCdr() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<String> msisdns = subscribers.msisdns();
        String msisdn = msisdns.get(random.nextInt(msisdns.size()));
        double roll = random.nextDouble();
        CdrType type = roll < 0.40 ? CdrType.VOICE : roll < 0.70 ? CdrType.SMS : CdrType.DATA;
        String country = type == CdrType.DATA ? "SA"
                : random.nextDouble() < internationalShare ? FOREIGN.get(random.nextInt(FOREIGN.size())) : "SA";
        String counterparty = type == CdrType.DATA ? "-" : "9665" + random.nextInt(10_000_000, 99_999_999);
        long duration = type == CdrType.VOICE ? random.nextLong(5, 600) : 0;
        long bytes = type == CdrType.DATA ? random.nextLong(100_000, 200_000_000) : 0;
        return new Cdr(UUID.randomUUID().toString(), type, msisdn, counterparty, country, duration, bytes,
                Instant.now(), cellId());
    }

    private static String cellId() {
        return String.format("CELL-%04d", ThreadLocalRandom.current().nextInt(1, CELLS + 1));
    }

    public record Status(boolean running, int ratePerSecond, long cdrsGenerated, long sendFailures) {}
}
