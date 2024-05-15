package live.clover.partition;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;

import java.io.Closeable;

/**
 * 分区器接口
 * @author weibb
 */
public interface Partitioner extends Configurable, Closeable {

    /**
     * 根据消息record信息对其进行重新分区
     *
     * @param topic 主题名称
     * @param key 用于分区的 key 对象
     * @param keyBytes 用于分区的 key 的二进制数组
     * @param value 生产者消息对象
     * @param valueBytes 生产者消息对象的二进制数组
     * @param cluster 当前 kafka 集群的 metadata 信息
     * @return 分区数量
     */
    int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster);

    /**
     * 当分区器执行完成时被调用
     */
    @Override
    void close();


    default void onNewBatch(String topic, Cluster cluster, int prevPartition) {}
}