package ut.bigdata.heatmap.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import ut.bigdata.heatmap.model.Room;

import java.util.List;
import java.util.Map;

public class RoomPartitioner implements Partitioner {
    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void close() {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Room room = (Room) key;

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int roomId = Integer.parseInt(room.getRoomId().split("_")[0]);
        int roomPartition = partitions.get(roomId % numPartitions).partition();

        return roomPartition;
    }
}
