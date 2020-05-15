package ut.bigdata.heatmap.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import ut.bigdata.heatmap.config.Config;
import ut.bigdata.heatmap.KafkaUtils;
import ut.bigdata.heatmap.SensorReading;
import ut.bigdata.heatmap.functions.JsonDeserializer;
import ut.bigdata.heatmap.functions.JsonGeneratorWrapper;
import ut.bigdata.heatmap.functions.TemperatureGenerator;
import ut.bigdata.heatmap.functions.TimeStamper;

import static ut.bigdata.heatmap.config.Parameters.SENSOR_SOURCE;
import static ut.bigdata.heatmap.config.Parameters.RECORDS_PER_SECOND;
import static ut.bigdata.heatmap.config.Parameters.DATA_TOPIC;

import java.util.Properties;

public class TemperatureSource {
    private static final int RULES_STREAM_PARALLELISM = 1;

    public static SourceFunction<String> createSensorReadingSource(Config config) {
        String sourceType = config.get(SENSOR_SOURCE);
        TemperatureSource.Type temperatureSourceType = TemperatureSource.Type.valueOf(sourceType.toUpperCase());

        int sensorReadingsPerSecond = config.get(RECORDS_PER_SECOND);

        switch (temperatureSourceType) {
            case KAFKA:
                Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
                String readingTopic = config.get(DATA_TOPIC);
                FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(readingTopic, new SimpleStringSchema(), kafkaProps);
                kafkaConsumer.setStartFromLatest();

                return kafkaConsumer;
            default:
                return new JsonGeneratorWrapper<>(new TemperatureGenerator(sensorReadingsPerSecond));
        }
    }

    public static DataStream<SensorReading> stringStreamToSensorReadings(DataStream<String> sensorReadingsSource) {
        return sensorReadingsSource
            .flatMap(new JsonDeserializer<SensorReading>(SensorReading.class))
            .returns(SensorReading.class)
            .flatMap(new TimeStamper<SensorReading>())
            .returns(SensorReading.class)
            .name("Sensor Reading Deserialization");
    }

    public enum Type {
        GENERATOR("Sensor readings generated locally"),
        KAFKA("Sensor source on Kafka");

        private String name;

        Type(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }
}
