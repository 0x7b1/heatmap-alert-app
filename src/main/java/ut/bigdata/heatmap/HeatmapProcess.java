package ut.bigdata.heatmap;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import ut.bigdata.heatmap.models.TemperatureRecord;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HeatmapProcess {
    static String inputTopic = "sensor_temperatures";
    static String consumerGroup = "sensor_consumer";
    static String kafkaAddress = "kafka:9092";
//    static String kafkaAddress = "localhost:29092";

    static String influxDBName = "sensor_temperatures";
    static String influxDBHost = "localhost:8086";
    //    static String influxDBHost = "http://localhost:8086";
    static String influxMeasurementAvg = "rooms_avg";
    static String influxMeasurementTemperatures = "rooms_temperatures";

    public static FlinkKafkaConsumer<TemperatureRecord> createTemperatureConsumer(String topic, String kafkaAddress, String consumerGroup) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

        return new FlinkKafkaConsumer<>(
            topic,
            new TemperatureRecordDeserializationSchema(),
            props);
    }

    public static class TemperatureRecordTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<TemperatureRecord> {
        public TemperatureRecordTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(TemperatureRecord temperatureRecord) {
            return temperatureRecord.getTimestamp();
        }
    }

    public static class TemperatureAverager implements WindowFunction<TemperatureRecord, Tuple3<String, Double, Long>, String, TimeWindow> {
        @Override
        public void apply(
            String roomId,
            TimeWindow timeWindow,
            Iterable<TemperatureRecord> iterable,
            Collector<Tuple3<String, Double, Long>> out) throws Exception {
            int sum = 0;
            int count = 0;

            for (TemperatureRecord recordInWindow : iterable) {
                sum += recordInWindow.getTemperature();
                count += 1;
            }

            Double avgWindowBySource = sum * 1.0 / count;
            Long windowTimestamp = timeWindow.getEnd();

            out.collect(Tuple3.of(roomId, avgWindowBySource, windowTimestamp));
        }
    }

    public static class JoinWindowsToTemperatureRelation implements JoinFunction<Tuple3<String, Double, Long>, Tuple3<String, Double, Long>, Tuple3<String, Double, Long>> {
        @Override
        public Tuple3<String, Double, Long> join(
            Tuple3<String, Double, Long> avgIn,
            Tuple3<String, Double, Long> avgOut) throws Exception {

            String roomId = avgIn.f0;
            Double indexRelation = avgIn.f1 / avgOut.f1;
            Long windowTimestamp = avgIn.f2;

            return Tuple3.of(roomId, indexRelation, windowTimestamp);
        }
    }

    public static InfluxDBSink createInfluxSink(String influxDBHost, String influxDBName) {
        InfluxDBConfig config = InfluxDBConfig.builder(
            influxDBHost,
            "root",
            "root",
            influxDBName)
            .batchActions(1000)
            .flushDuration(100, TimeUnit.MILLISECONDS)
            .enableGzip(true)
            .build();

        return new InfluxDBSink(config);
    }

    public static class TemperatureAvgRoomsToInfluxDataPoint implements MapFunction<Tuple3<String, Double, Long>, InfluxDBPoint> {
        String measurement = "";

        public TemperatureAvgRoomsToInfluxDataPoint(String measurementName) {
            this.measurement = measurementName;
        }

        @Override
        public InfluxDBPoint map(Tuple3<String, Double, Long> record) throws Exception {
            String measurement = this.measurement;
            Long timestamp = record.f2;
            HashMap<String, String> tags = new HashMap<>();
            HashMap<String, Object> fields = new HashMap<>();

            tags.put("room", record.f0);

            fields.put("value", record.f1);

            return new InfluxDBPoint(measurement, timestamp, tags, fields);
        }
    }

    public static class TemperatureRoomsToInfluxDataPoint implements MapFunction<Tuple3<String, Double, Long>, InfluxDBPoint> {
        String measurement = "";
        String source = "";

        public TemperatureRoomsToInfluxDataPoint(String measurementName, String source) {
            this.measurement = measurementName;
            this.source = source;
        }

        @Override
        public InfluxDBPoint map(Tuple3<String, Double, Long> record) throws Exception {
            String measurement = this.measurement;
            Long timestamp = record.f2;
            HashMap<String, String> tags = new HashMap<>();
            HashMap<String, Object> fields = new HashMap<>();

            tags.put("room", record.f0);
            tags.put("source", source);

            fields.put("value", record.f1);

            return new InfluxDBPoint(measurement, timestamp, tags, fields);
        }
    }

    public static void testKafkaConnection() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer<TemperatureRecord> kafkaConsumerSource =
            createTemperatureConsumer(inputTopic, kafkaAddress, consumerGroup);

        DataStream<TemperatureRecord> temperatureRecords = env
            .addSource(kafkaConsumerSource)
            .assignTimestampsAndWatermarks(new TemperatureRecordTimestampExtractor(Time.seconds(0)));

        temperatureRecords.print();

        env.execute("Relation between temperatures IN/OUT per room");
    }

    public static void runTemperatureStreaming() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer<TemperatureRecord> kafkaConsumerSource =
            createTemperatureConsumer(inputTopic, kafkaAddress, consumerGroup);

        DataStream<TemperatureRecord> temperatureRecords = env
            .addSource(kafkaConsumerSource)
            .assignTimestampsAndWatermarks(new TemperatureRecordTimestampExtractor(Time.seconds(0)));
        // This will spread messages from partitions evenly across flink workers
//            .rebalance();

        DataStream<Tuple3<String, Double, Long>> in = temperatureRecords
            .filter(e -> e.getSource().equals("IN"))
            .keyBy(e -> e.getRoomId())
            .timeWindow(Time.seconds(5))
            .apply(new TemperatureAverager());

        DataStream<Tuple3<String, Double, Long>> out = temperatureRecords
            .filter(e -> e.getSource().equals("OUT"))
            .keyBy(e -> e.getRoomId())
            .timeWindow(Time.seconds(5))
            .apply(new TemperatureAverager());


        DataStream<Tuple3<String, Double, Long>> temperatureRoomSourceAvgs = in
            .join(out)
            .where(e -> e.f0)
            .equalTo(e -> e.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(1)))
            .apply(new JoinWindowsToTemperatureRelation());

        // Sinking to InfluxDB
//        InfluxDBSink influxDBSink = createInfluxSink(influxDBHost, influxDBName);
//        temperatureRoomSourceAvgs
//            .map(new TemperatureAvgRoomsToInfluxDataPoint(influxMeasurementAvg));
//            .addSink(influxDBSink);

        // Sinking IN/OUT room temperatures
//        in.map(new TemperatureRoomsToInfluxDataPoint(influxMeasurementTemperatures, "IN")).addSink(influxDBSink);
//        out.map(new TemperatureRoomsToInfluxDataPoint(influxMeasurementTemperatures, "OUT")).addSink(influxDBSink);

        temperatureRoomSourceAvgs.print();
        System.out.println(env.getExecutionPlan());

        env.execute("Relation between temperatures IN/OUT per room");
    }

    public static void main(String[] args) throws Exception {
//        testKafkaConnection();
        runTemperatureStreaming();
    }
}
