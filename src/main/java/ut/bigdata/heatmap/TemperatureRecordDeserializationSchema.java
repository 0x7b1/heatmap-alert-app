package ut.bigdata.heatmap;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import ut.bigdata.heatmap.models.TemperatureRecord;

import java.util.Arrays;

public class TemperatureRecordDeserializationSchema implements KafkaDeserializationSchema<TemperatureRecord> {
    //public class TemperatureRecordDeserializationKeyedSchema implements KafkaDeserializationSchema<Tuple2<String, TemperatureRecord>> {
    static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public boolean isEndOfStream(TemperatureRecord temperatureRecord) {
        return false;
    }

    @Override
//    public Tuple2<String, TemperatureRecord> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
    public TemperatureRecord deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        ObjectNode node = objectMapper.createObjectNode();
        node.set("value", objectMapper.readValue(consumerRecord.value(), JsonNode.class));
        node.set("key", objectMapper.readValue(consumerRecord.key(), JsonNode.class));

        Long timestamp = node.get("value").get("timestamp").asLong();
        Integer temperature = node.get("value").get("value").asInt();
        String[] roomIdSource = node.get("key").get("roomId").asText().split("_");
        String roomId = roomIdSource[0] + "_" + roomIdSource[1];
        String source = roomIdSource[2];

        return new TemperatureRecord(roomId, source, timestamp, temperature);
    }

    @Override
    public TypeInformation<TemperatureRecord> getProducedType() {
//    public TypeInformation<Tuple2<String, TemperatureRecord>> getProducedType() {
//        return new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
//        return TypeInformation.of(new TypeHint<Tuple2<String, TemperatureRecord>>() {});
        return TypeInformation.of(TemperatureRecord.class);
    }
}

//        input.process(new ProcessFunction<Tuple2<String, TemperatureRecord>, TemperatureRecord>() {
//            @Override
//            public void processElement(Tuple2<String, TemperatureRecord> record, Context context, Collector<TemperatureRecord> out) throws Exception {
//                String topicName = record.f0;
//                out.collect(record.f1);
//            }
//        })
//        .print();
