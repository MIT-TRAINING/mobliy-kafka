package com.training.kafka.telco.plans;

import com.training.kafka.telco.common.JsonCodec;
import com.training.kafka.telco.model.PlanEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * Loads subscriber.plan into memory. Each application instance needs the WHOLE
 * table (it is a lookup table, not work to be shared), so this listener assigns
 * itself every partition and always starts at offset 0 instead of joining a
 * load-sharing consumer group.
 *
 * After compaction the topic holds far fewer records than were ever written, so
 * this start-up replay stays short however many plan changes happened.
 */
@Component
public class PlanCacheConsumer {

    private static final Logger log = LoggerFactory.getLogger(PlanCacheConsumer.class);

    private final PlanTable table;
    private final JsonCodec json;

    public PlanCacheConsumer(PlanTable table, JsonCodec json) {
        this.table = table;
        this.json = json;
    }

    @KafkaListener(id = "plan-cache",
            groupId = "#{topics.group('plan-cache')}",
            topicPartitions = @TopicPartition(topic = "#{topics.plan()}", partitions = "#{topics.planPartitions()}",
                    partitionOffsets = @PartitionOffset(partition = "*", initialOffset = "0")))
    public void onPlan(ConsumerRecord<String, String> record) {
        // A null value is a tombstone (Module 1 section 7.3): remove the subscriber
        PlanEvent event = record.value() == null ? null : json.fromJson(record.value(), PlanEvent.class);
        table.apply(record.key(), event);
        if (log.isDebugEnabled()) {
            log.debug("plan partition={} offset={} key={} value={}",
                    record.partition(), record.offset(), record.key(), record.value());
        }
    }
}
