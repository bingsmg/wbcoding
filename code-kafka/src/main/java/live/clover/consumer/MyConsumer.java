package live.clover.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author weibb
 */
public class MyConsumer {

    private static final Properties props;
    private static final KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        consume();
    }

    static {
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "47.101.204.168:9092"); // kafka集群信息
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "clover-group"); // 消费者组名称
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 关闭自动提交
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20"); // 一次拉取最大记录数，默认 500
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000"); // 一次处理数据时间周期
        /*
            Kafka 默认的消费组重置策略 (auto.offset.reset) 是 "latest"。这意味着，
            当消费者组在某个分区中没有已提交的偏移量时，它会从最新的偏移量开始消费数据，
            而不会处理任何已有的历史消息。如果你的消费者组是第一次消费某个主题，
            并且在它启动之前主题中已经有消息，那么默认策略下它不会消费这些历史消息。
         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 重置消费者组偏移量
        consumer = new KafkaConsumer<>(props);
    }

    public static void consume() {
        consumer.subscribe(Collections.singletonList("clover-topic"));
        try (consumer) {
            while (true) {
                // Duration超时时间，如果有数据可消费，立即返回数据
                // 如果没有数据可消费，超过 Duration 超时时间也会返回，但是返回结果数据量为0
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                records.forEach(MyConsumer::dealRecord);
                if (records.isEmpty()) {
                    System.out.println("No data need to be processed.");
                    break;
                }
                consumer.commitSync(Duration.ofSeconds(2));
            }
        } finally {
            consumer.close();
        }
    }

    //针对单条数据进行处理，此方法中应该做好异常处理，避免外围的while循环因为异常中断。
    private static void dealRecord(ConsumerRecord<String, String> record) {
        System.out.println("topic:" + record.topic()
                + ", partition:" + record.partition()
                + ", offset:" + record.offset()
                + ", key:" + record.key()
                + ", value:" + record.value());
    }
}
