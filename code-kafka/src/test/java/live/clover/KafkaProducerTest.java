package live.clover;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

/**
 * @author weibb
 */
public class KafkaProducerTest {

    private Properties props;

    @BeforeEach
    public void preTest() {
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "47.101.204.168:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    @Test
    public void noCallback() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 20; i++) {
            producer.send(new ProducerRecord<>("producer_test", Integer.toString(i), "noCallback value:" + i));
        }
        producer.close();
    }
}
