package ru.praktikum;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MyTopicProducer {
    public static void main(String[] args) {
        Properties config = KafkaConfig.load();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getProperty("bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        KafkaConfig.applySecurity(props, config);
        String topic = config.getProperty("kafka.topic");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 5; i++) {
                String key = "key-" + i;
                String value = "Тестовое сообщение " + i;
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, key, value);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("Отправлено в %s: partition=%d, offset=%d, key=%s%n",
                                topic, metadata.partition(), metadata.offset(), key);
                    } else {
                        System.err.println("Ошибка отправки: " + exception.getMessage());
                        exception.printStackTrace();
                    }
                });
            }
            producer.flush();
            System.out.println("Все сообщения отправлены в топик: " + topic);
        }
    }
}