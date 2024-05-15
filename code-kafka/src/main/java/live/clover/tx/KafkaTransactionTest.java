package live.clover.tx;

import live.clover.interceptor.MyProducerInterceptor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

import java.util.Properties;

/**
 * @author weibb
 */
public class KafkaTransactionTest {
    public static void main(String[] args) {
        //生产者参数配置
        KafkaProducer<String, String> producer = getKafkaProducer();

        try {
            String topic = "producer_test";
            String msg = "cm test";
            producer.beginTransaction(); //开启一个事务
            producer.send(new ProducerRecord<>(topic, "0", msg));
            producer.send(new ProducerRecord<>(topic, "1", msg));
            producer.send(new ProducerRecord<>(topic, "2", msg));
            producer.commitTransaction(); //提交事务
        } catch (KafkaException e2) {
            e2.printStackTrace();
            producer.abortTransaction();  //事务回滚
        } finally{
            producer.close();
        }
    }

    private static KafkaProducer<String, String> getKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("client.id", "ProducerTranscationnalExample");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("transactional.id", "test-transactional");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MyProducerInterceptor.class.getName());
        //生产者对象新建
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();  //初始化事务管理器
        return producer;
    }
}
