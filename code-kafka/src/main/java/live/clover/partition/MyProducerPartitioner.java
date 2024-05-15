package live.clover.partition;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * 自定义生产者分区器
 * @author weibb
 */
public class MyProducerPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //获取topic的partitions信息
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int partitionsNum = partitionInfos.size();
        //为特定的key自定义分区规则
        if (key != null && key.toString().equals("zimug")) {
            //分配到最后一个分区
            return partitionsNum - 1;
        }else{
            //分配到0号分区，即主题中的第一个分区
            return 0;
        }
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> map) {}
}
