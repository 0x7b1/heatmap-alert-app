package ut.bigdata.heatmap.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import ut.bigdata.heatmap.processors.SensorInOut;
import ut.bigdata.heatmap.processors.SensorReading;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HeatmapFixedSource {
    public static void main(String[] args) throws Exception {
//        fixedSources();
        streamLoopWithmetrics();
    }

    public static void streamLoopWithmetrics() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setParallelism(1);

        DataStream<SensorReading> inputStreamShort = env.addSource(new SourceFunction<SensorReading>() {
            boolean running = true;

            @Override
            public void run(SourceContext<SensorReading> out) throws Exception {
                while (running) {
                    int roomId = 0;
                    long timestamp = 20;
                    double temperature = 30;
                    SensorReading sensor = new SensorReading(roomId, timestamp, temperature, SensorInOut.IN);

                    out.collect(sensor);

                    Thread.sleep(1500);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });
//            .map(new RichMapFunction<SensorReading, SensorReading>() {
//            // A Meter measures an average throughput.
//            private transient Meter meter;
//            private transient Counter eventCounter;
//            private transient Histogram valueHistogram;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//                this.meter = getRuntimeContext()
//                    .getMetricGroup()
//                    .meter("myflink_throughput", new MeterView(5));
//
//                eventCounter = getRuntimeContext().getMetricGroup().counter("myflink_events");
//                valueHistogram =
//                    getRuntimeContext()
//                        .getMetricGroup()
//                        .histogram("myflink_histogram", new DescriptiveStatisticsHistogram(10000));
//            }
//
//            @Override
//            public SensorReading map(SensorReading sensorReading) throws Exception {
//                this.meter.markEvent();
//                eventCounter.inc();
//                valueHistogram.update(1);
//                return sensorReading;
//            }
//        });


        inputStreamShort.print();

        env.execute();
    }

    public static void fixedSources() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<SensorReading> sensorList = new ArrayList<>();
        sensorList.add(new SensorReading(1, 1, 29.0, SensorInOut.IN));
        sensorList.add(new SensorReading(1, 2, 30.0, SensorInOut.IN));
        sensorList.add(new SensorReading(1, 3, 31.0, SensorInOut.IN));
        sensorList.add(new SensorReading(1, 4, 32.0, SensorInOut.IN));

        DataStream<SensorReading> inputStreamShort = env.fromCollection(sensorList)
            .map(new RichMapFunction<SensorReading, SensorReading>() {
                // A Meter measures an average throughput.
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
                public SensorReading map(SensorReading sensorReading) throws Exception {
                    this.meter.markEvent();
                    eventCounter.inc();
                    valueHistogram.update(1);
                    return sensorReading;
                }
            });


        inputStreamShort.print();

        env.execute();
    }
}
