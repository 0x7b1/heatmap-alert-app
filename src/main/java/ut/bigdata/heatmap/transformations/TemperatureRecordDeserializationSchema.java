package ut.bigdata.heatmap.transformations;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import ut.bigdata.heatmap.models.TemperatureRecord;

import java.util.Arrays;

public class TemperatureRecordDeserializationSchema implements KafkaDeserializationSchema<TemperatureRecord> {
    static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public boolean isEndOfStream(TemperatureRecord temperatureRecord) {
        return false;
    }

    @Override
    public TemperatureRecord deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        ObjectNode node = objectMapper.createObjectNode();
        node.set("value", objectMapper.readValue(consumerRecord.value(), JsonNode.class));
        node.set("key", objectMapper.readValue(consumerRecord.key(), JsonNode.class));

        Long timestamp = node.get("value").get("timestamp").asLong();
        Integer temperature = node.get("value").get("value").asInt();
        String[] roomIdSource = node.get("key").get("roomId").asText().split("_");
        String roomId = roomIdSource[0];
        String source = roomIdSource[1];

        return new TemperatureRecord(roomId, source, timestamp, temperature);
    }

    @Override
    public TypeInformation<TemperatureRecord> getProducedType() {
        return TypeInformation.of(TemperatureRecord.class);
    }
}
