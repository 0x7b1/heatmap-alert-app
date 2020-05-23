package ut.bigdata.heatmap.processors;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import ut.bigdata.heatmap.config.Config;
import ut.bigdata.heatmap.source.TemperatureSource;

import java.util.concurrent.TimeUnit;

import static ut.bigdata.heatmap.config.Parameters.CHECKPOINT_INTERVAL;
import static ut.bigdata.heatmap.config.Parameters.MIN_PAUSE_BETWEEN_CHECKPOINTS;
import static ut.bigdata.heatmap.config.Parameters.SOURCE_PARALLELISM;
import static ut.bigdata.heatmap.config.Parameters.OUT_OF_ORDERNESS;

public class TemperatureEvaluator {
    private Config config;

    public TemperatureEvaluator(Config config) {
        this.config = config;
    }

    public void run() throws Exception {
        // Environment setup
        Configuration flinkConfig = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointInterval(config.get(CHECKPOINT_INTERVAL));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.get(MIN_PAUSE_BETWEEN_CHECKPOINTS));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));

        // Streams setup
        DataStream<SensorReading> sensorReadings = getSensorReadingStream(env);

        // Processing pipeline setup
//        DataStream<Alert> alerts = sensorReadings

    }

    public DataStream<SensorReading> getSensorReadingStream(StreamExecutionEnvironment env) {
        SourceFunction<String> sensorReadingsSource = TemperatureSource.createSensorReadingSource(config);
        int sourceParallelism = config.get(SOURCE_PARALLELISM);

        DataStream<String> sensorReadingsString = env.addSource(sensorReadingsSource)
            .name("Sensor Temperature Source")
            .setParallelism(sourceParallelism);

        DataStream<SensorReading> sensorReadingStream = TemperatureSource.stringStreamToSensorReadings(sensorReadingsString);
        return sensorReadingStream.assignTimestampsAndWatermarks(new SimpleBoundedOutOfOrdernessTimestampExtractor<>(config.get(OUT_OF_ORDERNESS)));
    }

    private static class SimpleBoundedOutOfOrdernessTimestampExtractor<T extends SensorReading>
        extends BoundedOutOfOrdernessTimestampExtractor<T> {

        public SimpleBoundedOutOfOrdernessTimestampExtractor(int outOfOrderdnessMillis) {
            super(Time.of(outOfOrderdnessMillis, TimeUnit.MILLISECONDS));
        }

        @Override
        public long extractTimestamp(T element) {
            return element.getTimestamp();
        }
    }
}
