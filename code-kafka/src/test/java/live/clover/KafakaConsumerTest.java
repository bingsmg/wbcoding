package live.clover;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * @author weibb
 */
public class KafakaConsumerTest {

    private Properties props;

    @BeforeEach
    public void preTest() {
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "47.101.204.168:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
//        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
//        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "static-member-id-1");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyProducerPartitioner.class.getName());
    }

    @Test
    public void pollData() {
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//        consumer.subscribe(List.of("topic_1"));
//        consumer.assign(List.of(new TopicPartition("topic_1", 0)));
//        Set<String> subscription = consumer.subscription();
        // seek 指定从 topic 分区的某个 offset 开始消费
//        consumer.seek(partition, 5);
        new Thread(() -> {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "cg1_topic_1");
            KafkaConsumer<String, String> consumer1 = new KafkaConsumer<>(props);
            consumer1.subscribe(List.of("topic_1"));
            System.out.println(consumer1.subscription());
            try (consumer1) {
                while (true) {
                    // 循环拉取数据,
                    // Duration 超时时间，如果有数据可消费，立即返回数据
                    // 如果没有数据可消费，超过 Duration 超时时间也会返回，但是返回结果数据量为 0
                    ConsumerRecords<String, String> records = consumer1.poll(Duration.ofSeconds(30));
                    if (records.isEmpty()) {
                        System.out.println("No records found");
                        return;
                    }
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("thread1 process message: ");
                        dealRecord(record);
                        System.out.println("##########################");
                    }
                }
            }
        }, "thread1").start();
        new Thread(() -> {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "cg2_topic_1");
            KafkaConsumer<String, String> consumer2 = new KafkaConsumer<>(props);
            consumer2.subscribe(List.of("topic_1"));
            System.out.println(consumer2.subscription());
            try (consumer2) {
                while (true) {
                    // 循环拉取数据,
                    // Duration 超时时间，如果有数据可消费，立即返回数据
                    // 如果没有数据可消费，超过 Duration 超时时间也会返回，但是返回结果数据量为 0
                    ConsumerRecords<String, String> records = consumer2.poll(Duration.ofSeconds(30));
                    if (records.isEmpty()) {
                        System.out.println("No records found");
                        return;
                    }
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("thread2 process message: ");
                        dealRecord(record);
                        System.out.println("##########################");
                    }
                }
            }
        }, "thread1").start();
//        System.out.println(subscription);
//        System.out.println(consumer.assignment());
//        try (consumer) {
//            while (true) {
//                // 循环拉取数据,
//                // Duration 超时时间，如果有数据可消费，立即返回数据
//                // 如果没有数据可消费，超过 Duration 超时时间也会返回，但是返回结果数据量为 0
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));
//                if (records.isEmpty()) {
//                    System.out.println("No records found");
//                    return;
//                }
//                for (ConsumerRecord<String, String> record : records) {
//                    dealRecord(record);
//                }
//            }
//        }
    }

    // 针对单条数据进行处理，此方法中应该做好异常处理，避免外围的 while 循环因为异常中断。
    private void dealRecord(ConsumerRecord<String, String> record) {
        System.out.println("topic:" + record.topic()
                + ",partition:" + record.partition()
                + ",offset:" + record.offset()
                + ",key:" + record.key()
                + ",value" + record.value());
    }

    private static void producer() {

    }
}
