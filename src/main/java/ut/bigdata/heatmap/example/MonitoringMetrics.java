package ut.bigdata.heatmap.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.ThreadLocalRandom;

public class MonitoringMetrics {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);
        env.disableOperatorChaining();

        env.addSource(new SourceFunction<Integer>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                while (isRunning) {
                    ctx.collect(ThreadLocalRandom.current().nextInt(0x7b1));
                    Thread.sleep(Time.seconds(1).toMilliseconds());
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).name("Random Source")
            .map(new RichMapFunction<Integer, Integer>() {
                private static final long serialVersionUID = 1L;
                private transient Counter eventCounter;
                private transient Histogram valueHistogram;

                @Override
                public void open(Configuration parameters) throws Exception {
                    eventCounter = getRuntimeContext().getMetricGroup().counter("mevents");
                    valueHistogram = getRuntimeContext().getMetricGroup().histogram("mhistogram", new DescriptiveStatisticsHistogram(10_000));
                }

                @Override
                public Integer map(Integer value) throws Exception {
                    eventCounter.inc();
                    valueHistogram.update(value);
                    return value;
                }
            }).name("Map with metrics")
            .print();
//            .addSink(new DiscardingSink<>()) // TODO: Replace with kafka
//            .name("Discarding sink");

        env.execute("Exposing Metrics Job");
    }
}
