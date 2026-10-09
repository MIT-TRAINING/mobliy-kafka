package com.examples.spring.boot.kafka.avro;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class SpringBootKafkaAvroApplication {

	private static final Logger logger = LoggerFactory.getLogger(SpringBootKafkaAvroApplication.class);

	private final KafkaTemplate<String, GenericRecord> template;
	private final String topic;
	private final String schemaVersion;
	private final Schema schema;

	public SpringBootKafkaAvroApplication(KafkaTemplate<String, GenericRecord> template,
			@Value("${app.topic}") String topic, @Value("${app.schema}") String schemaVersion) {
		this.template = template;
		this.topic = topic;
		this.schemaVersion = schemaVersion;
		this.schema = CdrRecords.load(schemaVersion);
		logger.info("Producer writes to '{}' with schema {} ({} fields)", topic, schemaVersion,
				schema.getFields().size());
	}

	public static void main(String[] args) {
		SpringApplication.run(SpringBootKafkaAvroApplication.class, args);
	}

	// curl -X POST localhost:7073/cdr -H 'Content-Type: application/json' \
	//   -d '{"callId":"c-1","caller":"9198450001","callee":"9198450002","durationSec":42}'
	@PostMapping("/cdr")
	public ResponseEntity<String> publish(@RequestBody Map<String, Object> json) {
		GenericRecord cdr;
		try {
			cdr = CdrRecords.fromJson(schema, json);
		} catch (RuntimeException e) {
			// e.g. a required field is missing from the JSON: Avro refuses it before Kafka sees it
			return ResponseEntity.badRequest().body("Not a valid CdrVoice " + schemaVersion + ": " + e.getMessage() + "\n");
		}
		try {
			SendResult<String, GenericRecord> result = template.send(topic, cdr.get("caller").toString(), cdr)
					.get(15, TimeUnit.SECONDS);
			String where = result.getRecordMetadata().topic() + "-" + result.getRecordMetadata().partition()
					+ "@" + result.getRecordMetadata().offset();
			logger.info("Sent {} with schema {} to {}", cdr, schemaVersion, where);
			return ResponseEntity.ok("Sent with schema " + schemaVersion + " to " + where + "\n");
		} catch (Exception e) {
			// Schema Registry refused the schema (incompatible, 401, 403) or Kafka refused the record
			Throwable root = e;
			while (root.getCause() != null) {
				root = root.getCause();
			}
			logger.error("Send failed: {}", root.toString());
			return ResponseEntity.status(500).body("Send failed: " + root + "\n");
		}
	}

	@KafkaListener(topics = "${app.topic}", groupId = "${app.prefix}.cdr-avro-reader")
	public void listen(ConsumerRecord<String, GenericRecord> record) {
		GenericRecord cdr = record.value();
		// GenericRecord carries the schema the producer wrote with (looked up by ID in the registry)
		logger.info("Received {}-{}@{} key={} fields={} value={}", record.topic(), record.partition(),
				record.offset(), record.key(), cdr.getSchema().getFields().size(), cdr);
	}
}
