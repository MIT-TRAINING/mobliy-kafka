package com.training.kafka.telco.common;

import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Records travel through Kafka as plain JSON text (String serializer), so
 * kafka-console-consumer.sh can read every topic of this case study. Schemas
 * and Avro/Schema Registry are a later module.
 */
@Component
public class JsonCodec {

    private final JsonMapper mapper;

    public JsonCodec(JsonMapper mapper) {
        this.mapper = mapper;
    }

    public String toJson(Object value) {
        return mapper.writeValueAsString(value);
    }

    public <T> T fromJson(String json, Class<T> type) {
        return mapper.readValue(json, type);
    }
}
