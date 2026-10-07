package com.training.kafka.telco.topics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Confluent Cloud answers a rejected topic setting with a bare
 * "PolicyViolationException: Request parameters do not satisfy the configured policy",
 * without naming the topic or the setting. Before KafkaAdmin applies the catalog, this
 * check asks the cluster about every topic and every setting on its own, with
 * validateOnly=true (nothing is created or changed), and fails with the exact list.
 *
 * CLI equivalent: confluent kafka topic create T --config K=V --dry-run
 */
final class CloudTopicPreflight {

    private static final Logger log = LoggerFactory.getLogger(CloudTopicPreflight.class);
    private static final long TIMEOUT_SECONDS = 30;

    private CloudTopicPreflight() {
    }

    static void check(Map<String, Object> adminConfig, List<TopicSpec> specs) {
        List<String> problems = new ArrayList<>();
        try (Admin admin = Admin.create(adminConfig)) {
            Set<String> existing = admin.listTopics().names().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            for (TopicSpec spec : specs) {
                if (existing.contains(spec.name())) {
                    checkAlter(admin, spec, problems);
                } else {
                    checkCreate(admin, spec, problems);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while validating topics", e);
        } catch (Exception e) {
            // Connection or authentication problems: let KafkaAdmin report them as usual
            log.warn("Confluent Cloud topic preflight skipped: {}", e.toString());
            return;
        }
        if (!problems.isEmpty()) {
            throw new IllegalStateException("Confluent Cloud rejected these topic settings "
                    + "(PolicyViolationException). Adjust the Cloud branch of TopicCatalog:\n  "
                    + String.join("\n  ", problems));
        }
        log.info("Confluent Cloud accepts all {} topic designs", specs.size());
    }

    /** New topic: try it as a whole; if refused, without configs, then one config at a time. */
    private static void checkCreate(Admin admin, TopicSpec spec, List<String> problems) throws Exception {
        if (createRejection(admin, spec, spec.configs()) == null) {
            return;
        }
        String bare = createRejection(admin, spec, Map.of());
        if (bare != null) {
            problems.add(spec.name() + ": partitions=" + spec.partitions() + ", replicas=" + spec.replicas()
                    + " -> " + bare);
            return;
        }
        spec.configs().forEach((key, value) -> {
            try {
                String reason = createRejection(admin, spec, Map.of(key, value));
                if (reason != null) {
                    problems.add(spec.name() + ": " + key + "=" + value + " -> " + reason);
                }
            } catch (Exception e) {
                problems.add(spec.name() + ": " + key + "=" + value + " -> " + e);
            }
        });
    }

    private static String createRejection(Admin admin, TopicSpec spec, Map<String, String> configs) throws Exception {
        NewTopic topic = new NewTopic(spec.name(), spec.partitions(), (short) spec.replicas()).configs(configs);
        try {
            admin.createTopics(List.of(topic), new CreateTopicsOptions().validateOnly(true))
                    .all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof PolicyViolationException) {
                return e.getCause().getMessage();
            }
            throw e;
        }
    }

    /** Existing topic (e.g. lNN.cdr.voice from Module 6 Lab 01): KafkaAdmin will alter it, so validate that. */
    private static void checkAlter(Admin admin, TopicSpec spec, List<String> problems) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, spec.name());
        spec.configs().forEach((key, value) -> {
            try {
                admin.incrementalAlterConfigs(
                                Map.of(resource, List.of(new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET))),
                                new AlterConfigsOptions().validateOnly(true))
                        .all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof PolicyViolationException) {
                    problems.add(spec.name() + " (existing): " + key + "=" + value + " -> " + e.getCause().getMessage());
                } else {
                    problems.add(spec.name() + " (existing): " + key + "=" + value + " -> " + e.getCause());
                }
            } catch (Exception e) {
                problems.add(spec.name() + " (existing): " + key + "=" + value + " -> " + e);
            }
        });
    }
}
