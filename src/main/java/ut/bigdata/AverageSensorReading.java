package ut.bigdata;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AverageSensorReading {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env
            .addSource(new SensorSource())
            .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<SensorReading> avgTemp = sensorData
            .map(r -> new SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)))
            .keyBy(r -> r.id)
            .timeWindow(Time.seconds(1))
            .apply(new TemperatureAverager());

        avgTemp.print();

        env.execute("average sensor temperature");
    }

    public static class TemperatureAverager implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {
        @Override
        public void apply(String sensorId, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out) throws Exception {
            int count = 0;
            double sum = .0;
            for (SensorReading r : input) {
                count++;
                sum += r.temperature;
            }
            double avgTemp = sum / count;

            out.collect(new SensorReading(sensorId, window.getEnd(), avgTemp));
        }
    }
}
