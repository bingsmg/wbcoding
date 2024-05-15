package live.clover.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义 kafaka 拦截器统计消息发送成功失败次数
 * @author weibb
 */
public class MyProducerInterceptor implements ProducerInterceptor<Object, Object> {
    AtomicInteger successCnt = new AtomicInteger();
    AtomicInteger failureCnt = new AtomicInteger();

    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> producerRecord) {
        Object newValue = producerRecord.value();
        return new ProducerRecord<>(producerRecord.topic(), producerRecord.partition(), producerRecord.timestamp(),
                producerRecord.key(), newValue);
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (recordMetadata == null) {
            failureCnt.getAndIncrement();
        } else {
            successCnt.getAndIncrement();
        }
    }

    @Override
    public void close() {
        double successRate = (double) successCnt.get() / (successCnt.get() + failureCnt.get());
        System.out.println("message send success rate: " + successRate * 100 + "%");
    }

    @Override
    public void configure(Map<String, ?> map) {
        //
    }
}
