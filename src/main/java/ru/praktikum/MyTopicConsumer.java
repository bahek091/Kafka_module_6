package ru.praktikum;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MyTopicConsumer {
    public static void main(String[] args) {
        Properties config = KafkaConfig.load();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaConfig.getRequired(config, "bootstrap.servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                KafkaConfig.getRequired(config, "kafka.group.id"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConfig.applySecurity(props, config);

        String topic = KafkaConfig.getRequired(config, "kafka.topic");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook triggered, waking up consumer...");
            consumer.wakeup();
        }));

        try {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Waiting for messages from topic: " + topic);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received from %s: key=%s, value=%s, partition=%d, offset=%d%n",
                            topic, record.key(), record.value(),
                            record.partition(), record.offset());
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            System.out.println("Consumer poll interrupted for shutdown");
        } finally {
            consumer.close();
            System.out.println("Consumer closed gracefully");
        }
    }
}
