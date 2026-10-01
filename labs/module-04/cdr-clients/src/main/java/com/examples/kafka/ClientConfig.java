package com.examples.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection and security settings come from the same file the CLI tools use
 * with --command-config: ~/kafka/apache.properties (bootstrap.servers + SASL/SCRAM).
 * Behaviour (acks, linger.ms, group.id ...) is set in code on top of it.
 *
 * Use another file with -Dkafka.config=/path/to/file.properties
 * (for example a local Module 2 cluster).
 */
public final class ClientConfig {

    private static final Logger log = LoggerFactory.getLogger(ClientConfig.class);

    private ClientConfig() { }

    public static Properties baseConfig() throws IOException {
        Path cfg = Path.of(System.getProperty("kafka.config",
                Path.of(System.getProperty("user.home"), "kafka", "apache.properties").toString()));
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(cfg)) {
            props.load(in);                                  // bootstrap.servers + security settings
        }
        log.info("Connection settings from {} (bootstrap.servers={})", cfg, props.getProperty("bootstrap.servers"));
        return props;
    }

    /** Applies trailing "name=value" arguments, e.g. linger.ms=0 batch.size=16384. */
    public static void applyOverrides(Properties props, List<String> overrides) {
        for (String o : overrides) {
            int eq = o.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException("Expected config=value, got: " + o);
            }
            props.put(o.substring(0, eq), o.substring(eq + 1));
        }
    }
}
