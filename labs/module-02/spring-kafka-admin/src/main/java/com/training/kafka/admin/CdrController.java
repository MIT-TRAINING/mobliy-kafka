package com.training.kafka.admin;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Producing from Java (the application equivalent of kafka-console-producer.sh)
 * and switching the billing listener on and off to create consumer lag.
 */
@RestController
@RequestMapping("/api")
public class CdrController {

    private final KafkaTemplate<String, String> template;
    private final KafkaListenerEndpointRegistry registry;
    private final String topic;

    public CdrController(KafkaTemplate<String, String> template, KafkaListenerEndpointRegistry registry,
                         @Value("${lab.topics.cdr}") String topic) {
        this.template = template;
        this.registry = registry;
        this.topic = topic;
    }

    /**
     * One record, key = MSISDN. Same as typing "966500000001:call-start" into
     *   kafka-console-producer.sh --reader-property parse.key=true --reader-property key.separator=:
     */
    @PostMapping("/cdr")
    public SentView send(@RequestBody CdrRequest request) throws Exception {
        SendResult<String, String> result = template.send(topic, request.msisdn(), request.event())
                .get(10, TimeUnit.SECONDS);
        RecordMetadata meta = result.getRecordMetadata();
        return new SentView(meta.topic(), meta.partition(), meta.offset(), request.msisdn(), request.event());
    }

    /**
     * COUNT records spread over 10 subscribers (keys 966500000000..966500000009).
     * Returns how many records landed in each partition.
     */
    @PostMapping("/cdr/generate")
    public Map<Integer, Integer> generate(@RequestParam(defaultValue = "100") int count) {
        Map<Integer, Integer> perPartition = new TreeMap<>();
        CompletableFuture<?>[] sends = new CompletableFuture<?>[count];
        for (int i = 0; i < count; i++) {
            String msisdn = "96650000000" + (i % 10);
            String event = (i % 2 == 0 ? "call-start" : "call-end") + " seq=" + i;
            sends[i] = template.send(topic, msisdn, event).thenAccept(r -> {
                synchronized (perPartition) {
                    perPartition.merge(r.getRecordMetadata().partition(), 1, Integer::sum);
                }
            });
        }
        CompletableFuture.allOf(sends).join();
        return perPartition;
    }

    /** Stop the @KafkaListener: its consumers leave the group, so the group becomes Empty. */
    @PostMapping("/listener/stop")
    public Map<String, Object> stopListener() {
        billingContainer().stop();
        return listenerStatus();
    }

    @PostMapping("/listener/start")
    public Map<String, Object> startListener() {
        billingContainer().start();
        return listenerStatus();
    }

    @GetMapping("/listener")
    public Map<String, Object> listenerStatus() {
        MessageListenerContainer container = billingContainer();
        return Map.of("listenerId", CdrListener.LISTENER_ID, "running", container.isRunning());
    }

    private MessageListenerContainer billingContainer() {
        return registry.getListenerContainer(CdrListener.LISTENER_ID);
    }

    public record CdrRequest(String msisdn, String event) {}

    public record SentView(String topic, int partition, long offset, String key, String value) {}
}
