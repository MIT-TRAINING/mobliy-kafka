package com.training.kafka.telco.topics;

import java.util.Map;

/**
 * The written contract of one topic: what it carries, how many partitions and
 * copies, and every non-default setting with the reason for it.
 *
 * @param name          real topic name (with the learner prefix, if any)
 * @param workloadClass the class of data, from Module 3 section 4.1
 * @param purpose       what the topic is for, in business words
 * @param whyConfigured why these numbers, in one sentence
 * @param partitions    partition count
 * @param replicas      replication factor
 * @param configs       topic-level overrides (no "log." prefix, Module 3 section 4.2)
 */
public record TopicSpec(String name, String workloadClass, String purpose, String whyConfigured,
                        int partitions, int replicas, Map<String, String> configs) {
}
