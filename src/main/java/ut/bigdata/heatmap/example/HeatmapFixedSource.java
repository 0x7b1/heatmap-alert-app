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
import ut.bigdata.heatmap.processors.SensorInOut;
import ut.bigdata.heatmap.processors.SensorReading;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HeatmapFixedSource {
    public static void main(String[] args) throws Exception {
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
