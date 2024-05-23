package live.clover.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author weibb
 */
public class MyConsumerInterceptor implements ConsumerInterceptor<String, String> {

    // 数据处理总耗时
    private static final AtomicLong totalLatency = new AtomicLong();

    // 消息的总数量
    private static final AtomicLong msgCount = new AtomicLong();

    // 在消费者进行数据处理之前被调用
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        long lantency = 0L;
        for (ConsumerRecord<String, String> record : records) {
            lantency += (System.currentTimeMillis() - record.timestamp());
        }
        long totalLatencyLong = totalLatency.addAndGet(lantency); //加lantency
        long msgCountLong = msgCount.incrementAndGet(); //加1
        System.out.println("该批次消息发出到消费处理的平均延时：" + (totalLatencyLong / msgCountLong));
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
