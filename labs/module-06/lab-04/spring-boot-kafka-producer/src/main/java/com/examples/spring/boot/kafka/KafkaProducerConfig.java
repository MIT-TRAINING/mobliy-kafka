package com.examples.spring.boot.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

@Configuration
public class KafkaProducerConfig {

	  @Value("${spring.kafka.bootstrap-servers}")
	  private String bootstrapServers;

	  // Optional client file, e.g. ~/kafka/ccloud.properties: bootstrap.servers, security.protocol,
	  // sasl.mechanism, sasl.jaas.config. Empty = plain local cluster.
	  @Value("${app.kafka.client-config}")
	  private String clientConfig;

	  @Bean
	  public Map<String, Object> producerConfigs() {
	    Map<String, Object> props = new HashMap<>();
	    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	    props.putAll(loadClientConfig(clientConfig));
	    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);

	    return props;
	  }
	  
	  @Bean
	  public ProducerFactory<String, String> strProducerFactory() {
	    return new DefaultKafkaProducerFactory<>(producerConfigs());
	  }

	  @Bean
	  public KafkaTemplate<String, String> strKafkaTemplate() {
		  return new KafkaTemplate<>(strProducerFactory());
	  }	  

	  @Bean
	  public ProducerFactory<String, Greeting> objProducerFactory() {
	    return new DefaultKafkaProducerFactory<>(producerConfigs());
	  }

	  @Bean
	  public KafkaTemplate<String, Greeting> objKafkaTemplate() {
		  return new KafkaTemplate<>(objProducerFactory());
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
