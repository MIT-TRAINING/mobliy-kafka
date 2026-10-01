package com.examples.kafka.billing;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Blocking retry, then dead-letter topic (guide §9.4). Spring Boot plugs this
 * bean into the auto-configured listener container factory.
 */
@Configuration
public class ErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandlingConfig.class);

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template,
                                            @Value("${lab.topics.dlt}") String dlt) {
        // Same partition number in the DLT: create it with at least as many partitions as the source
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (rec, ex) -> new TopicPartition(dlt, rec.partition()));
        // Keep the DLT readable with kafka-console-consumer.sh: no stack trace, and the
        // original offset as text (Spring's own partition/offset headers are binary numbers)
        recoverer.excludeHeader(HeadersToAdd.EX_STACKTRACE, HeadersToAdd.PARTITION, HeadersToAdd.OFFSET,
                HeadersToAdd.TS, HeadersToAdd.TS_TYPE);
        recoverer.setHeadersFunction((rec, ex) -> new RecordHeaders().add("dlt.original.offset",
                String.valueOf(rec.offset()).getBytes(StandardCharsets.UTF_8)));
        ConsumerRecordRecoverer toDlt = (rec, ex) -> {
            recoverer.accept(rec, ex);
            log.error("Gave up on p={} off={} key={}: sent to {}", rec.partition(), rec.offset(), rec.key(), dlt);
        };
        // 1 attempt + 3 retries, 1 s apart; the container does not commit past the record until then
        DefaultErrorHandler handler = new DefaultErrorHandler(toDlt, new FixedBackOff(1000L, 3L));
        handler.addNotRetryableExceptions(IllegalArgumentException.class);    // bad data: DLT at once
        handler.setRetryListeners((rec, ex, attempt) ->
                log.warn("Attempt {} failed for p={} off={}: {}", attempt, rec.partition(), rec.offset(),
                        ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage()));
        return handler;
    }
}
