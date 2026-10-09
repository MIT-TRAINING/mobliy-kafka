package com.examples.spring.boot.kafka.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

@Configuration
@EnableKafka
public class KafkaAvroConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	// Kafka client file, as in Labs 04-05: bootstrap.servers, security.protocol, sasl.*, ssl.truststore.*
	@Value("${app.kafka.client-config}")
	private String clientConfig;

	// Schema Registry client file: schema.registry.url, basic.auth.*, schema.registry.ssl.truststore.*
	@Value("${app.sr.client-config}")
	private String srClientConfig;

	private Map<String, Object> commonConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.putAll(loadClientConfig(clientConfig));
		props.putAll(loadClientConfig(srClientConfig));
		return props;
	}

	@Bean
	public KafkaTemplate<String, GenericRecord> avroKafkaTemplate() {
		Map<String, Object> props = commonConfigs();
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		// Registers the writer schema under <topic>-value (if new) and writes
		// magic byte 0 + 4-byte schema ID + Avro binary
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
	}

	@Bean
	public ConsumerFactory<String, GenericRecord> avroConsumerFactory() {
		Map<String, Object> props = commonConfigs();
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		// Reads the schema ID from each record and fetches that schema from the registry (cached)
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, GenericRecord> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(avroConsumerFactory());
		return factory;
	}

	static Map<String, Object> loadClientConfig(String path) {
		Map<String, Object> config = new HashMap<>();
		if (path == null || path.isBlank()) {
			return config;
		}
		Properties file = new Properties();
		try (InputStream in = Files.newInputStream(Path.of(path))) {
			file.load(in);
		} catch (IOException e) {
			throw new UncheckedIOException("Cannot read client config " + path, e);
		}
		file.forEach((key, value) -> config.put((String) key, value));
		return config;
	}
}
