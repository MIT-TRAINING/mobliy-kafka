package com.training.kafka.telco.topics;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The catalog is the Module 3 design on paper. These tests guard the rules the
 * course teaches, so a careless edit to a topic contract fails the build.
 */
class TopicCatalogTest {

    private final TopicCatalog catalog = new TopicCatalog("", true);

    private TopicSpec spec(String name) {
        return catalog.specs().stream().filter(s -> s.name().equals(name)).findFirst().orElseThrow();
    }

    @Test
    void everyBusinessTopicIsDurable() {
        for (String name : new String[] {catalog.voice(), catalog.sms(), catalog.data(), catalog.charges(),
                catalog.plan(), catalog.audit()}) {
            assertThat(spec(name).replicas()).as(name + " replication factor").isEqualTo(3);
            assertThat(spec(name).configs()).as(name).containsEntry("min.insync.replicas", "2");
        }
    }

    @Test
    void telemetryDeliberatelyTradesDurabilityForCost() {
        TopicSpec telemetry = spec(catalog.telemetry());

        assertThat(telemetry.replicas()).isEqualTo(2);
        assertThat(telemetry.configs()).containsEntry("min.insync.replicas", "1");
        // topic must not recompress what the producer already compressed (Module 3 section 7.2)
        assertThat(telemetry.configs()).containsEntry("compression.type", "producer");
    }

    @Test
    void subscriberStateIsCompacted() {
        assertThat(spec(catalog.plan()).configs()).containsEntry("cleanup.policy", "compact");
    }

    @Test
    void topicLevelNamesNeverCarryTheBrokerLogPrefix() {
        catalog.specs().forEach(spec ->
                assertThat(spec.configs().keySet()).as(spec.name()).noneMatch(key -> key.startsWith("log.")));
    }

    @Test
    void learnerPrefixIsAppliedToTopicsAndGroups() {
        TopicCatalog prefixed = new TopicCatalog("l07.", false);

        assertThat(prefixed.voice()).isEqualTo("l07.cdr.voice");
        assertThat(prefixed.group("billing")).isEqualTo("l07.billing");
        assertThat(prefixed.names()).allMatch(name -> name.startsWith("l07."));
    }

    @Test
    void confluentCloudCatalogStaysInsideCloudRules() {
        for (boolean lab : new boolean[] {true, false}) {
            TopicCatalog cloud = new TopicCatalog("l07.", lab, true);
            cloud.specs().forEach(spec -> {
                // Cloud fixes RF at 3 and rejects these settings with PolicyViolationException
                assertThat(spec.replicas()).as(spec.name() + " RF").isEqualTo(3);
                assertThat(spec.configs()).as(spec.name())
                        .doesNotContainKeys("compression.type", "min.cleanable.dirty.ratio");
                assertThat(spec.configs().get("min.insync.replicas")).as(spec.name()).isIn("1", "2");
                String segmentMs = spec.configs().get("segment.ms");
                if (segmentMs != null) {
                    assertThat(Long.parseLong(segmentMs)).as(spec.name() + " segment.ms").isGreaterThanOrEqualTo(600_000L);
                }
                String segmentBytes = spec.configs().get("segment.bytes");
                if (segmentBytes != null) {
                    assertThat(Long.parseLong(segmentBytes)).as(spec.name() + " segment.bytes")
                            .isBetween(52_428_800L, 1_073_741_824L);
                }
                String maxLag = spec.configs().get("max.compaction.lag.ms");
                if (maxLag != null) {
                    assertThat(Long.parseLong(maxLag)).as(spec.name() + " max.compaction.lag.ms").isGreaterThanOrEqualTo(21_600_000L);
                }
            });
            // the business contracts survive the move to Cloud
            assertThat(cloud.specs().stream().filter(s -> s.name().equals(cloud.charges())).findFirst().orElseThrow()
                    .configs()).containsEntry("min.insync.replicas", "2");
            assertThat(cloud.specs().stream().filter(s -> s.name().equals(cloud.plan())).findFirst().orElseThrow()
                    .configs()).containsEntry("cleanup.policy", "compact");
        }
    }

    @Test
    void baseNamesResolveToPrefixedTopics() {
        TopicCatalog prefixed = new TopicCatalog("l07.", true);

        assertThat(prefixed.resolve("network.telemetry")).isEqualTo("l07.network.telemetry");
        assertThat(prefixed.resolve("l07.network.telemetry")).isEqualTo("l07.network.telemetry");
    }

    @Test
    void labModeOnlyChangesTheCompactionTimings() {
        TopicCatalog production = new TopicCatalog("", false);

        assertThat(spec(catalog.plan()).configs().get("segment.ms")).isEqualTo("30000");
        assertThat(production.specs().stream().filter(s -> s.name().equals("subscriber.plan")).findFirst().orElseThrow()
                .configs().get("segment.ms")).isEqualTo("3600000");
    }
}
