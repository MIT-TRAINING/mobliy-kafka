package com.examples.kafka.review;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.examples.kafka.ClientConfig;

/**
 * Lab 01 Part 6: a first draft, the way an AI assistant might suggest it for
 * "a Kafka producer that sends 10 JSON CDRs keyed by MSISDN, make it fast".
 *
 * It compiles. Run it, then review it against the guide (§2.1, §2.2, §3.3, §3.4)
 * and fix it. Do not copy this into real code.
 */
public class CopilotDraftProducer {

    public static void run(String topic) throws Exception {
        Properties props = ClientConfig.baseConfig();
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "1");                              // "faster than all"
        props.put("enable.idempotence", "true");             // "no duplicates"

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            String key = String.valueOf(new Random().nextInt());
            String value = "{\"msisdn\":\"96650000000" + i + "\",\"event\":\"call-start\"}";
            producer.send(new ProducerRecord<>(topic, key, value));
        }
        System.out.println("Sent 10 CDRs to " + topic);
    }
}
