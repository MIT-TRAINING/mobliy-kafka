package com.examples.spring.boot.kafka.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * Checks the lab's story without a cluster: v2 is a BACKWARD-compatible change of v1,
 * v3-breaking is not, and the serializer writes magic byte + schema ID + Avro.
 */
class CdrRecordsTest {

	private static final Map<String, Object> CALL = Map.of("callId", "c-1", "caller", "9198450001",
			"callee", "9198450002", "durationSec", 42);

	@Test
	void v2AddsAFieldWithADefaultSoItCanReadV1Data() {
		assertThat(SchemaCompatibility.checkReaderWriterCompatibility(CdrRecords.load("v2"), CdrRecords.load("v1"))
				.getType()).isEqualTo(SchemaCompatibilityType.COMPATIBLE);
		assertThat(CdrRecords.fromJson(CdrRecords.load("v2"), CALL).get("roaming")).isEqualTo(false);
	}

	@Test
	void v3AddsARequiredFieldSoItCannotReadV2Data() {
		assertThat(SchemaCompatibility.checkReaderWriterCompatibility(CdrRecords.load("v3-breaking"),
				CdrRecords.load("v2")).getType()).isEqualTo(SchemaCompatibilityType.INCOMPATIBLE);
	}

	@Test
	void missingRequiredFieldIsRefused() {
		assertThatThrownBy(() -> CdrRecords.fromJson(CdrRecords.load("v1"), Map.of("callId", "c-2")))
				.hasMessageContaining("caller");
	}

	@Test
	void serializerWritesMagicByteAndSchemaId() throws Exception {
		Map<String, Object> config = Map.of("schema.registry.url", "mock://lab06");
		Schema v1 = CdrRecords.load("v1");
		try (Serializer<Object> serializer = new KafkaAvroSerializer();
				Deserializer<Object> deserializer = new KafkaAvroDeserializer()) {
			serializer.configure(config, false);
			deserializer.configure(config, false);

			byte[] bytes = serializer.serialize("l07.cdr.voice.avro", CdrRecords.fromJson(v1, CALL));

			assertThat(bytes[0]).as("magic byte").isEqualTo((byte) 0);
			GenericRecord back = (GenericRecord) deserializer.deserialize("l07.cdr.voice.avro", bytes);
			assertThat(back.get("durationSec")).isEqualTo(42);
			assertThat(back.get("caller").toString()).isEqualTo("9198450001");
		}
		// default naming: one subject per topic, <topic>-value
		assertThat(MockSchemaRegistry.getClientForScope("lab06").getAllSubjects())
				.containsExactly("l07.cdr.voice.avro-value");
	}
}
