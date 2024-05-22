package live.clover;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * @author weibb
 */
public class Main {

    private static Properties props;
    static {
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "47.101.204.168:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cg_2");
    }
    public static void main( String[] args ) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("topic_2"));
        System.out.println(consumer.subscription());
        try (consumer) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty()) {
                System.out.println("No record!");
            }
            records.forEach(Main::dealRecord);
            for (ConsumerRecord<String, String> record : records) {
                dealRecord(record);
                consumer.commitSync(Duration.ofSeconds(5));
            }
        }
    }


    private static void dealRecord(ConsumerRecord<String, String> record) {
        System.out.println("topic:" + record.topic()
                + ",partition:" + record.partition()
                + ",offset:" + record.offset()
                + ",key:" + record.key()
                + ",value" + record.value());
    }
}
