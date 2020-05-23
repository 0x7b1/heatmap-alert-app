package ut.bigdata.heatmap;

import org.apache.flink.api.common.functions.JoinFunction;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import ut.bigdata.heatmap.models.TemperatureRecord;

import java.util.Properties;

public class HeatmapProcess {
    static String inputTopic = "sensor_temperatures";
    static String consumerGroup = "sensor_consumer";
    static String kafkaAddress = "localhost:29092";

    public static FlinkKafkaConsumer<TemperatureRecord> createTemperatureConsumer(String topic, String kafkaAddress, String consumerGroup) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaAddress);
        props.put("group.id", consumerGroup);

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

    public static class TemperatureAverager implements WindowFunction<TemperatureRecord, Tuple3<String, Integer, String>, String, TimeWindow> {
        @Override
        public void apply(
            String key,
            TimeWindow timeWindow,
            Iterable<TemperatureRecord> iterable,
            Collector<Tuple3<String, Integer, String>> out) throws Exception {
            int sum = 0;
            int count = 0;
            StringBuilder debug = new StringBuilder();
            String source = "";

            for (TemperatureRecord recordInWindow : iterable) {
                sum += recordInWindow.getTemperature();
                count += 1;
                debug.append(recordInWindow.getTemperature()).append(", ");
                source = recordInWindow.getSource();
            }

            debug.append(key).append(" ~ ").append(source);

            int avg = sum / count;
            out.collect(Tuple3.of(key, avg, debug.toString()));
        }
    }

    public static class JoinWindowsToTemperatureRelation implements JoinFunction<Tuple3<String, Integer, String>, Tuple3<String, Integer, String>, String> {
        @Override
        public String join(
            Tuple3<String, Integer, String> avgIn,
            Tuple3<String, Integer, String> avgOut) throws Exception {

            System.out.println("IN->" + avgIn);
            System.out.println("OUT->" + avgOut);

            return "JODA";
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        FlinkKafkaConsumer<TemperatureRecord> kafkaConsumerSource =
            createTemperatureConsumer(inputTopic, kafkaAddress, consumerGroup);

        DataStream<TemperatureRecord> temperatureRecords = env
            .addSource(kafkaConsumerSource)
            .assignTimestampsAndWatermarks(new TemperatureRecordTimestampExtractor(Time.seconds(0)));

        DataStream<Tuple3<String, Integer, String>> in = temperatureRecords
            .filter(e -> e.getSource().equals("IN"))
            .keyBy(e -> e.getRoomId())
            .timeWindow(Time.seconds(5))
            .apply(new TemperatureAverager());

        DataStream<Tuple3<String, Integer, String>> out = temperatureRecords
            .filter(e -> e.getSource().equals("OUT"))
            .keyBy(e -> e.getRoomId())
            .timeWindow(Time.seconds(5))
            .apply(new TemperatureAverager());


        in.join(out)
            .where(e -> e.f0)
            .equalTo(e -> e.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(1)))
            .apply(new JoinWindowsToTemperatureRelation())
            .print();

        System.out.println(env.getExecutionPlan());

        env.execute();
    }
}
