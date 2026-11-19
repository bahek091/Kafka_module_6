package ru.praktikum;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MyTopicConsumer {
    public static void main(String[] args) {
        Properties config = KafkaConfig.load();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getProperty("bootstrap.servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                config.getProperty("kafka.group.id"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConfig.applySecurity(props, config);
        String topic = config.getProperty("kafka.topic");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Ожидание сообщений из топика: " + topic);

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Получено из %s: key=%s, value=%s, partition=%d, offset=%d%n",
                            topic, record.key(), record.value(),
                            record.partition(), record.offset());
                }
            }
        }
    }
}