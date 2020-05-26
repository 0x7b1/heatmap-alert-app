package ut.bigdata.heatmap.example;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import ut.bigdata.heatmap.models.TemperatureRecord;
import ut.bigdata.heatmap.transformations.DataBuilder;
import ut.bigdata.heatmap.transformations.TemperatureRecordTimestampExtractor;

public class TestKafkaConnection {
    static String inputTopic = "sensor_temperatures";
    static String consumerGroup = "sensor_consumer";
    static String kafkaAddress = "localhost:29092";

    public static void main() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer<TemperatureRecord> kafkaConsumerSource =
            DataBuilder.createTemperatureConsumer(inputTopic, kafkaAddress, consumerGroup);

        DataStream<TemperatureRecord> temperatureRecords = env
            .addSource(kafkaConsumerSource)
            .assignTimestampsAndWatermarks(new TemperatureRecordTimestampExtractor(Time.seconds(0)));

        temperatureRecords.print();

        env.execute("Relation between temperatures IN/OUT per room");
    }
}
