package live.clover;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author weibb
 */
public class KafkaProducerTest {

    private Properties props;

    @BeforeEach
    public void preTest() {
        props = new Properties();
        // kafka broker服务列表
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "47.101.204.168:9092");
        // 可靠性确认应答参数
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // 发送失败，重新尝试的次数
//        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2000);
        //生产者数据 key 的格式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //生产者数据 value 的格式
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyProducerPartitioner.class.getName());
//        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    }

    @Test
    public void noCallback() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 20; i++) {
            producer.send(new ProducerRecord<>("producer_test", Integer.toString(i), "noCallback value:" + i));
            System.out.println("send success, current record: " + i);
        }
        producer.close();
    }

    @Test
    public void withCallback() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 20; i++) {
            producer.send(new ProducerRecord<>("topic_2", 0, null, "message withCallback value:" + i),
                    (metadata, exception) -> {  //回调函数
                        if (null == exception) {
                            System.out.println("消息数据发送成功 ->" + metadata.offset());
                        } else {
                            exception.printStackTrace();
                        }
                    });
        }

        producer.close();
    }

    //同步数据send
    @Test
    public void syncSend() throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        RecordMetadata metadata = producer.send(new ProducerRecord<>(
                        "topic_1",
                        "syncSend value" ))
                .get();
        if(metadata != null && metadata.hasOffset()){
            System.out.println("数据同步发送确认成功 ->" + metadata.offset());
        }
        producer.close();
    }
}
