package com.examples.kafka.billing;

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The producer side: KafkaTemplate is a KafkaProducer configured from
 * spring.kafka.producer.* (acks=all, idempotence, zstd, linger.ms=10).
 */
@RestController
@RequestMapping("/api")
public class CdrController {

    private final KafkaTemplate<String, String> template;
    private final String topic;

    public CdrController(KafkaTemplate<String, String> template, @Value("${lab.topics.cdr}") String topic) {
        this.template = template;
        this.topic = topic;
    }

    /** One CDR, key = MSISDN. Waits for the broker's acknowledgement and returns where it landed. */
    @PostMapping("/cdr")
    public SentView send(@RequestBody CdrRequest request) throws Exception {
        String cdr = "{\"cdrId\":\"" + UUID.randomUUID().toString().substring(0, 8) + "\",\"msisdn\":\"" + request.msisdn()
                + "\",\"type\":\"voice\",\"durationSec\":" + request.durationSec() + "}";
        return sendAndWait(request.msisdn(), cdr);
    }

    /** Any text as the value, e.g. a malformed record (a poison pill) for Lab 03. */
    @PostMapping("/cdr/raw")
    public SentView sendRaw(@RequestParam String key, @RequestBody String value) throws Exception {
        return sendAndWait(key, value);
    }

    /** COUNT CDRs over 20 subscribers (966500000000 ... 966500000019); returns records per partition. */
    @PostMapping("/cdr/generate")
    public Map<Integer, Integer> generate(@RequestParam(defaultValue = "100") int count) {
        Map<Integer, Integer> perPartition = new TreeMap<>();
        CompletableFuture<?>[] sends = new CompletableFuture<?>[count];
        for (int i = 0; i < count; i++) {
            String msisdn = String.format("9665000000%02d", i % 20);
            String cdr = "{\"cdrId\":\"" + UUID.randomUUID().toString().substring(0, 8) + "\",\"msisdn\":\"" + msisdn
                    + "\",\"type\":\"voice\",\"durationSec\":" + (30 + i % 300) + "}";
            // The future fails if the record is finally not written: never ignore it (guide §8.5)
            sends[i] = template.send(topic, msisdn, cdr).thenAccept(r -> {
                synchronized (perPartition) {
                    perPartition.merge(r.getRecordMetadata().partition(), 1, Integer::sum);
                }
            });
        }
        CompletableFuture.allOf(sends).join();
        return perPartition;
    }

    private SentView sendAndWait(String key, String value) throws Exception {
        RecordMetadata meta = template.send(topic, key, value).get(10, TimeUnit.SECONDS).getRecordMetadata();
        return new SentView(meta.topic(), meta.partition(), meta.offset(), key, value);
    }

    public record CdrRequest(String msisdn, int durationSec) {}

    public record SentView(String topic, int partition, long offset, String key, String value) {}
}
