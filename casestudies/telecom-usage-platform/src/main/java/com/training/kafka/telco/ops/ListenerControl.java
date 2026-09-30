package com.training.kafka.telco.ops;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

/**
 * Start and stop the consumers of this application. Stopping a listener makes
 * its consumers leave the group: the group goes Empty, records pile up, lag
 * grows. Starting it again resumes from the committed offsets and catches up.
 */
@Service
public class ListenerControl {

    static final List<String> IDS = List.of("billing", "fraud-detection", "usage-analytics");

    private final KafkaListenerEndpointRegistry registry;

    public ListenerControl(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    public Map<String, Boolean> status() {
        Map<String, Boolean> status = new LinkedHashMap<>();
        for (String id : IDS) {
            MessageListenerContainer container = registry.getListenerContainer(id);
            status.put(id, container != null && container.isRunning());
        }
        return status;
    }

    public Map<String, Boolean> stop(String id) {
        container(id).stop();
        return status();
    }

    public Map<String, Boolean> start(String id) {
        MessageListenerContainer container = container(id);
        if (!container.isRunning()) {
            container.start();
        }
        return status();
    }

    private MessageListenerContainer container(String id) {
        MessageListenerContainer container = IDS.contains(id) ? registry.getListenerContainer(id) : null;
        if (container == null) {
            throw new IllegalArgumentException("Unknown listener '" + id + "'. Known: " + IDS);
        }
        return container;
    }
}
