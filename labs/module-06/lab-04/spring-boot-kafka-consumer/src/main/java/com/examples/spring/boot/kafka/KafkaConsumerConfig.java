package com.examples.spring.boot.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	// Optional client file, e.g. ~/kafka/ccloud.properties: bootstrap.servers, security.protocol,
	// sasl.mechanism, sasl.jaas.config. Empty = plain local cluster.
	@Value("${app.kafka.client-config}")
	private String clientConfig;

	@Bean
	public Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.putAll(loadClientConfig(clientConfig));
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);

		return props;
	}

	// Replaces Spring Boot's default listener factory, which only reads spring.kafka.*.
	// The String listeners (test, demo) use it.
	@Bean
	public ConsumerFactory<String, String> stringConsumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(), new StringDeserializer());
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(stringConsumerFactory());
		return factory;
	}

	@Bean
	public ConsumerFactory<String, Greeting> greetingConsumerFactory() {
		JacksonJsonDeserializer<Greeting> deserializer = new JacksonJsonDeserializer<>(Greeting.class);
		deserializer.addTrustedPackages("com.examples.spring.boot.kafka");
		return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(), deserializer);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Greeting> greetingKafkaListenerContainerFactory() {

		ConcurrentKafkaListenerContainerFactory<String, Greeting> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(greetingConsumerFactory());
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
			throw new UncheckedIOException("Cannot read Kafka client config " + path, e);
		}
		file.forEach((key, value) -> config.put((String) key, value));
		return config;
	}
}
