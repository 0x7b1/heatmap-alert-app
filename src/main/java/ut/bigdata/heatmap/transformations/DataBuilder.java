package ut.bigdata.heatmap.transformations;

import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import ut.bigdata.heatmap.transformations.TemperatureRecordDeserializationSchema;
import ut.bigdata.heatmap.models.TemperatureRecord;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class DataBuilder {
    public static InfluxDBSink createInfluxSink(String influxDBHost, String influxDBName, String influxDBUser, String influxDBPassword) {
        InfluxDBConfig config = InfluxDBConfig.builder(
            influxDBHost,
            influxDBUser,
            influxDBPassword,
            influxDBName)
            .batchActions(1000)
            .flushDuration(100, TimeUnit.MILLISECONDS)
            .enableGzip(true)
            .build();

        return new InfluxDBSink(config);
    }

    public static FlinkKafkaConsumer<TemperatureRecord> createTemperatureConsumer(String topic, String kafkaAddress, String consumerGroup) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

        return new FlinkKafkaConsumer<>(
            topic,
            new TemperatureRecordDeserializationSchema(),
            props);
    }
}
