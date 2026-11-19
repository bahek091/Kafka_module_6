package ru.praktikum;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaConfig {
    public static Properties load() {
        Properties props = new Properties();
        try (InputStream input = KafkaConfig.class.getClassLoader()
                .getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new IllegalStateException(
                        "Файл config.properties не найден в classpath");
            }
            props.load(input);
            return props;
        } catch (IOException e) {
            throw new RuntimeException("Не удалось загрузить config.properties", e);
        }
    }

    public static void applySecurity(Properties target, Properties source) {
        target.put("security.protocol", "SASL_SSL");
        target.put("sasl.mechanism", "SCRAM-SHA-512");
        target.put("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                        "username=\"" + source.getProperty("sasl.username") + "\" " +
                        "password=\"" + source.getProperty("sasl.password") + "\";");
        target.put("ssl.truststore.location",
                source.getProperty("ssl.truststore.location"));
        target.put("ssl.truststore.password",
                source.getProperty("ssl.truststore.password"));
    }
}