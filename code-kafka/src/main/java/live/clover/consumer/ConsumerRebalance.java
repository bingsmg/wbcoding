package live.clover.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * @author weibb
 */
public class ConsumerRebalance {

    private static final KafkaConsumer<String, String> consumer;
    private static final Map<TopicPartition, OffsetAndMetadata> currentOffsetMap = new HashMap<>();

    public static void main(String[] args) {

        while (true) {
            // 这里的参数指的是轮询的时间间隔，也就是多长时间去拉一次数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
            records.forEach((ConsumerRecord<String, String> record) -> {
                System.out.println("topic:" + record.topic()
                        + ",partition:" + record.partition()
                        + ",offset:" + record.offset()
                        + ",key:" + record.key()
                        + ",value:" + record.value());
                // 每次消费记录消费偏移量，用于一旦发生rebalance时提交
                currentOffsetMap.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no matadata"));
            });
            consumer.commitAsync();
        }
    }

    static {
        Properties configs = initConfig();
        consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(List.of("topic-rebalance"), new ConsumerRebalanceListener() {
            // 在 rebalance 发生之前和消费者停止读取消息之后被调用。
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                consumer.commitSync(currentOffsetMap);
            }
            // 在rebalance完成之后(重新分配了消费者对应的分区)，消费者开始读取消息之前被调用。
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                consumer.commitSync(currentOffsetMap);
            }
        });
    }

    private static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "clover-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }
}
