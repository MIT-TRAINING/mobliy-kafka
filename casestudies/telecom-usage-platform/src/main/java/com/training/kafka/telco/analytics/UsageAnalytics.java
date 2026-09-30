package com.training.kafka.telco.analytics;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import com.training.kafka.telco.model.Cdr;
import com.training.kafka.telco.model.CdrType;
import org.springframework.stereotype.Component;

/** Usage counters for the operations dashboard. */
@Component
public class UsageAnalytics {

    private final Map<CdrType, LongAdder> recordsByType = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> recordsByPartition = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> voiceSecondsBySubscriber = new ConcurrentHashMap<>();
    private final LongAdder voiceSeconds = new LongAdder();
    private final LongAdder dataBytes = new LongAdder();
    private final LongAdder international = new LongAdder();

    void record(String topic, int partition, Cdr cdr) {
        recordsByType.computeIfAbsent(cdr.type(), t -> new LongAdder()).increment();
        recordsByPartition.computeIfAbsent(topic + "-" + partition, p -> new LongAdder()).increment();
        if (cdr.type() == CdrType.VOICE) {
            voiceSeconds.add(cdr.durationSec());
            voiceSecondsBySubscriber.computeIfAbsent(cdr.msisdn(), m -> new LongAdder()).add(cdr.durationSec());
        }
        if (cdr.type() == CdrType.DATA) {
            dataBytes.add(cdr.bytes());
        }
        if (cdr.international()) {
            international.increment();
        }
    }

    public Summary summary(int top) {
        Map<String, Long> byType = new TreeMap<>();
        recordsByType.forEach((type, count) -> byType.put(type.name(), count.sum()));
        Map<String, Long> byPartition = new TreeMap<>();
        recordsByPartition.forEach((partition, count) -> byPartition.put(partition, count.sum()));
        List<TopTalker> talkers = voiceSecondsBySubscriber.entrySet().stream()
                .map(e -> new TopTalker(e.getKey(), e.getValue().sum()))
                .sorted(Comparator.comparingLong(TopTalker::voiceSeconds).reversed())
                .limit(top)
                .toList();
        return new Summary(byType, byPartition, voiceSeconds.sum(), dataBytes.sum() / 1_048_576, international.sum(),
                talkers);
    }

    public record TopTalker(String msisdn, long voiceSeconds) {}

    public record Summary(Map<String, Long> recordsByType, Map<String, Long> recordsByPartition, long voiceSeconds,
                          long dataMegabytes, long internationalRecords, List<TopTalker> topTalkers) {}
}
