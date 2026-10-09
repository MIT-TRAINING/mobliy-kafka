package com.examples.spring.boot.kafka.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

/**
 * Builds Avro records from the schema files in src/main/resources/avro/.
 *
 * The lab uses GenericRecord instead of classes generated from the schema, so you
 * can switch schema versions with SCHEMA=v1|v2|v3-breaking without rebuilding.
 */
public final class CdrRecords {

	private CdrRecords() {
	}

	public static Schema load(String version) {
		String resource = "/avro/cdr-voice-" + version + ".avsc";
		try (InputStream in = CdrRecords.class.getResourceAsStream(resource)) {
			if (in == null) {
				throw new IllegalArgumentException("No schema " + resource + " (use v1, v2 or v3-breaking)");
			}
			return new Schema.Parser().parse(in);
		} catch (IOException e) {
			throw new UncheckedIOException("Cannot read " + resource, e);
		}
	}

	/**
	 * Copies the JSON fields that the schema knows into a record. A field missing from
	 * the JSON gets the schema default; without a default, build() fails.
	 */
	public static GenericRecord fromJson(Schema schema, Map<String, Object> json) {
		GenericRecordBuilder builder = new GenericRecordBuilder(schema);
		for (Schema.Field field : schema.getFields()) {
			Object value = json.get(field.name());
			if (value == null) {
				continue;
			}
			builder.set(field, switch (field.schema().getType()) {
				case INT -> ((Number) value).intValue();
				case LONG -> ((Number) value).longValue();
				case STRING -> value.toString();
				default -> value;
			});
		}
		GenericData.Record record = builder.build();
		return record;
	}
}
