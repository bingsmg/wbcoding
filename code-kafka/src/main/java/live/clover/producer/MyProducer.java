package live.clover.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Objects;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author weibb
 */
public class MyProducer {
    private static final String TOPIC_NAME = "clover-topic";
    private static final KafkaProducer<String, String> producer;

    public static void main(String[] args) {
        MyProducer.produceWithCallback();
    }

    static  {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "47.101.204.168:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 保证所有副本都同步到 可选[0,1...]
        props.put(ProducerConfig.RETRIES_CONFIG, 1); // 发送失败重试次数
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2000); // 请求超时时间
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyProducerPartitioner.class.getName());
        producer = new KafkaProducer<>(props);
    }

    private static void produceWithCallback() {
        for (int i = 0; i < 20; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME,
                    null,
                    "message with callback value" + i);
            producer.send(record, run()); // call back
        }
        producer.close();
    }

    private static void produceNoCallback() {
        for (int i = 0; i < 20; i++) {
            producer.send(new ProducerRecord<>(TOPIC_NAME, "noCallback value:" + i));
            System.out.println("send success, current record: " + i);
        }
        producer.close();
    }

    private static Callback run() {
        return (metadata, exception) -> {
            if (Objects.isNull(exception)) {
                System.out.println("data send success, current data offset is " + metadata.offset());
            } else {
                System.out.println(exception.getMessage());
            }
        };
    }
}
