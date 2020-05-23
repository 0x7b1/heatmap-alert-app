package ut.bigdata.heatmap.processors;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class HeatmapStream {
    private static final Double TEMPERATURE_THRESHOLD = 32.0;
    private static final Double TEMPERATURE_MEAN = 38.068689;
    private static final Double TEMPERATURE_STD = 2.147318;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Input stream of IN sensor events
        DataStream<SensorReading> inputStreamSensorIn = env
            .addSource(new SensorEventSource(SensorInOut.IN, 10, TEMPERATURE_MEAN, TEMPERATURE_STD))
            .name("SensorIN")
            .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

//        inputStreamSensorIn.print();

        // Create a pattern of two consecutive events going above the threshold within the defined interval
        Pattern<SensorReading, ?> warningPattern = Pattern
            .<SensorReading>begin("first")
            .subtype(SensorReading.class)
            .where(new IterativeCondition<SensorReading>() {
                @Override
                public boolean filter(SensorReading sensorReading, Context<SensorReading> context) throws Exception {
                    return sensorReading.getTemperature() >= TEMPERATURE_THRESHOLD;
                }
            })
            .next("second")
            .subtype(SensorReading.class)
            .where(new IterativeCondition<SensorReading>() {
                @Override
                public boolean filter(SensorReading sensorReading, Context<SensorReading> context) throws Exception {
                    return sensorReading.getTemperature() >= TEMPERATURE_THRESHOLD;
                }
            })
            .within(Time.seconds(10)); // Time.hours(1)

        // Create a pattern stream for the warning pattern
        PatternStream<SensorReading> warningPatternStream = CEP.pattern(
            inputStreamSensorIn.keyBy("roomId"),
            warningPattern
        );

        // Generate temperature warnings for each matched warning pattern
        DataStream<TemperatureWarning> warnings = warningPatternStream.select(
            (Map<String, List<SensorReading>> pattern) -> {
                SensorReading first = (SensorReading) pattern.get("first").get(0);
                SensorReading second = (SensorReading) pattern.get("second").get(0);

                double avgTemperatures = first.getTemperature() + second.getTemperature() / 2;
                return new TemperatureWarning(first.getRoomId(), avgTemperatures);
            });

        // Create an alert if two consecutive warnings appear in the given interval
        Pattern<TemperatureWarning, ?> alertPattern = Pattern
            .<TemperatureWarning>begin("first")
            .next("second")
            .within(Time.seconds(10));

        // Create a pattern stream for the alert pattern
        PatternStream<TemperatureWarning> alertPatternStream = CEP.pattern(
            warnings.keyBy("roomId"),
            alertPattern
        );

        // Generate temperature alert only if the second temperature warning avg is higher than the first one
        DataStream<TemperatureAlert> alerts = alertPatternStream.flatSelect(
            (Map<String, List<TemperatureWarning>> pattern, Collector<TemperatureAlert> collector) -> {
                TemperatureWarning first = pattern.get("first").get(0);
                TemperatureWarning second = pattern.get("second").get(0);

                if (first.getAvgTemperature() < second.getAvgTemperature()) {
                    collector.collect(new TemperatureAlert(first.getRoomId()));
                }
            },
            TypeInformation.of(TemperatureAlert.class)
        ).map(new RichMapFunction<TemperatureAlert, TemperatureAlert>() {
            private transient Meter meter;
            private transient Counter eventCounter;
            private transient Histogram valueHistogram;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.meter = getRuntimeContext()
                    .getMetricGroup()
                    .meter("throughput", new MeterView(5));

                eventCounter = getRuntimeContext().getMetricGroup().counter("events");
                valueHistogram =
                    getRuntimeContext()
                        .getMetricGroup()
                        .histogram("value_histogram", new DescriptiveStatisticsHistogram(10000));
            }

            @Override
            public TemperatureAlert map(TemperatureAlert temperatureAlert) throws Exception {
                this.meter.markEvent();
                return temperatureAlert;
            }
        });

        warnings.print();
        alerts.print();

        env.execute("Heatmap event alert system");

        // Split the stream into outside/inside temperature streams
        // Join the stream to compare the temperatures per room
        // Define interval of interests
        // Enrich the streams with the room metadata
    }
}
