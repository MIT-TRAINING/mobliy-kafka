package com.training.kafka.admin;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * The application equivalent of
 *   kafka-console-consumer.sh --topic cdr.data --group cdr-billing \
 *     --formatter-property print.partition=true --formatter-property print.offset=true
 *
 * concurrency = 3 starts three consumers in the same group, so the 6 partitions
 * of cdr.data are shared out 2 per consumer (thread).
 */
@Component
public class CdrListener {

    static final String LISTENER_ID = "cdr-billing";

    private static final Logger log = LoggerFactory.getLogger(CdrListener.class);

    @KafkaListener(id = LISTENER_ID, groupId = "cdr-billing", topics = "${lab.topics.cdr}", concurrency = "3")
    public void onCdr(ConsumerRecord<String, String> record) {
        log.info("partition={} offset={} key={} value={}",
                record.partition(), record.offset(), record.key(), record.value());
    }
}
