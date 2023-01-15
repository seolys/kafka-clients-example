package seol.study;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {
    private final static String BOOTSTRAP_SERVER = "localhost:9092";
    private final static String GROUP_ID = "group_one";
    private final static String TOPIC_NAME = "topic5";

    public static void main(final String[] args) {
        final Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(List.of(TOPIC_NAME));

        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (final ConsumerRecord<String, String> record : records) {
                System.out.println(">>> " + record);
            }
        }
    }
}
