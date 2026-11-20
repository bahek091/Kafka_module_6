package ru.praktikum;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaConfig {

    public static Properties load() {
        Properties props = new Properties();
        try (InputStream input = KafkaConfig.class.getClassLoader()
                .getResourceAsStream("config.properties")) {
            if (input != null) props.load(input);
            return props;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config.properties", e);
        }
    }

    public static String get(Properties config, String key) {
        String sysValue = System.getProperty(key);
        if (sysValue != null && !sysValue.isBlank()) return sysValue;

        String envName = key.toUpperCase().replace('.', '_').replace('-', '_');
        String envValue = System.getenv(envName);
        if (envValue != null && !envValue.isBlank()) return envValue;

        return config != null ? config.getProperty(key) : null;
    }

    public static String getRequired(Properties config, String key) {
        String value = get(config, key);
        if (value == null || value.isBlank()) {
            String envName = key.toUpperCase().replace('.', '_').replace('-', '_');
            throw new IllegalStateException(
                    "Required parameter '" + key + "' is missing. " +
                            "Set env " + envName + ", -D" + key + "=..., or config.properties");
        }
        return value;
    }

    public static void applySecurity(Properties target, Properties config) {
        target.put("security.protocol", "SASL_SSL");
        target.put("sasl.mechanism", "SCRAM-SHA-512");
        target.put("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                        "username=\"" + getRequired(config, "sasl.username") + "\" " +
                        "password=\"" + getRequired(config, "sasl.password") + "\";");
        target.put("ssl.truststore.location", getRequired(config, "ssl.truststore.location"));
        target.put("ssl.truststore.password", getRequired(config, "ssl.truststore.password"));
    }
}
