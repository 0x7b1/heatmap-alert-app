package ut.bigdata.heatmap.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class InfluxSinkTest {
    public static InfluxDBSink createInfluxSink() {
        InfluxDBConfig config = InfluxDBConfig.builder(
            "http://localhost:8086",
            "root",
            "root",
            "db_flink_test")
            .batchActions(1000)
            .flushDuration(100, TimeUnit.MILLISECONDS)
            .enableGzip(true)
            .build();

        return new InfluxDBSink(config);
    }

    // curl -POST http://localhost:8086/query --data-urlencode "q=CREATE DATABASE sensor_temperatures"
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple4<String, String, Integer, Long>> data = env.fromElements(
            Tuple4.of("1", "out", 10, ZonedDateTime.now().minusSeconds(10).toEpochSecond() * 1000),
            Tuple4.of("1", "in", 11, ZonedDateTime.now().minusSeconds(7).toEpochSecond() * 1000),
            Tuple4.of("2", "in", 9, ZonedDateTime.now().minusSeconds(4).toEpochSecond() * 1000),
            Tuple4.of("2", "out", 8, ZonedDateTime.now().minusSeconds(1).toEpochSecond() * 1000));

        DataStream<InfluxDBPoint> dataInflux = data.map(new MapFunction<Tuple4<String, String, Integer, Long>, InfluxDBPoint>() {
            @Override
            public InfluxDBPoint map(Tuple4<String, String, Integer, Long> record) throws Exception {
                String measurement = "newrooms";
                Long timestamp = record.f3;
                HashMap<String, String> tags = new HashMap<>();
                HashMap<String, Object> fields = new HashMap<>();

                tags.put("room", record.f0);
                tags.put("source", record.f1);

                fields.put("temp", record.f2);

                return new InfluxDBPoint(measurement, timestamp, tags, fields);
            }
        });

        InfluxDBSink influxDBSink = createInfluxSink();
        dataInflux.addSink(influxDBSink);

        env.execute();
    }
}
