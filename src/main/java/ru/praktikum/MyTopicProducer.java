package ru.praktikum;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MyTopicProducer {
    public static void main(String[] args) {
        Properties config = KafkaConfig.load();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaConfig.getRequired(config, "bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        KafkaConfig.applySecurity(props, config);

        String topic = KafkaConfig.getRequired(config, "kafka.topic");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 5; i++) {
                String key = "key-" + i;
                String value = "Test message " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("Sent to %s: partition=%d, offset=%d, key=%s%n",
                                topic, metadata.partition(), metadata.offset(), key);
                    } else {
                        System.err.println("Send failed: " + exception.getMessage());
                        exception.printStackTrace();
                    }
                });
            }
            producer.flush();
            System.out.println("All messages sent to topic: " + topic);
        }
    }
}
