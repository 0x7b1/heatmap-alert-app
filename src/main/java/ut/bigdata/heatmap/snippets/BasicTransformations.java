package ut.bigdata.heatmap.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Random;


class SensorSource extends RichParallelSourceFunction<SensorReadingX> {
    // flag indicating whether source is still running
    private boolean running = true;

    /** run() continuously emits SensorReadingXs by emitting them through the SourceContext. */
    @Override
    public void run(SourceFunction.SourceContext<SensorReadingX> srcCtx) throws Exception {

        // initialize random number generator
        Random rand = new Random();
        // look up index of this parallel task
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();

        // initialize sensor ids and temperatures
        String[] sensorIds = new String[10];
        double[] curFTemp = new double[10];
        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "sensor_" + (taskIdx * 10 + i);
            curFTemp[i] = 65 + (rand.nextGaussian() * 20);
        }

        while (running) {

            // get current time
            long curTime = Calendar.getInstance().getTimeInMillis();

            // emit SensorReadingXs
            for (int i = 0; i < 10; i++) {
                // update current temperature
                curFTemp[i] += rand.nextGaussian() * 0.5;
                // emit reading
                srcCtx.collect(new SensorReadingX(sensorIds[i], curTime, curFTemp[i]));
            }

            // wait for 100 ms
            Thread.sleep(100);
        }
    }

    /** Cancels this SourceFunction. */
    @Override
    public void cancel() {
        this.running = false;
    }
}

class SensorReadingX {
    // id of the sensor
    public String id;
    // timestamp of the reading
    public long timestamp;
    // temperature value of the reading
    public double temperature;

    /**
     * Empty default constructor to satify Flink's POJO requirements.
     */
    public SensorReadingX() { }

    public SensorReadingX(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String toString() {
        return "(" + this.id + ", " + this.timestamp + ", " + this.temperature + ")";
    }
}

class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<SensorReadingX> {

    /**
     * Configures the extractor with 5 seconds out-of-order interval.
     */
    public SensorTimeAssigner() {
        super(Time.seconds(5));
    }

    /**
     * Extracts timestamp from SensorReading.
     *
     * @param r sensor reading
     * @return the timestamp of the sensor reading.
     */
    @Override
    public long extractTimestamp(SensorReadingX r) {
        return r.timestamp;
    }
}


public class BasicTransformations {
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        // ingest sensor stream
        DataStream<SensorReadingX> readings = env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource())
            // assign timestamps and watermarks which are required for event time
            .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        // filter out sensor measurements with temperature below 25 degrees
        DataStream<SensorReadingX> filteredReadings = readings
            .filter(r -> r.temperature >= 25);

        // the above filter transformation using a FilterFunction instead of a lambda function
        // DataStream<SensorReadingX> filteredReadings = readings
        //     .filter(new TemperatureFilter(25));

        // project the reading to the id of the sensor
        DataStream<String> sensorIds = filteredReadings
            .map(r -> r.id);

        // the above map transformation using a MapFunction instead of a lambda function
        // DataStream<String> sensorIds = filteredReadings
        //     .map(new IdExtractor());

        // split the String id of each sensor to the prefix "sensor" and sensor number
//        DataStream<String> splitIds = sensorIds
//            .flatMap((FlatMapFunction<String, String>)
//                    (id, out) -> { for (String s: id.split("_")) { out.collect(s);}})
//            // provide result type because Java cannot infer return type of lambda function
//            .returns(Types.STRING);

        // the above flatMap transformation using a FlatMapFunction instead of a lambda function
         DataStream<String> splitIds = sensorIds
                 .flatMap(new IdSplitter());

        // print result stream to standard out
        splitIds.print();

        // execute application
        env.execute("Basic Transformations Example");
    }

    /**
     * User-defined FilterFunction to filter out SensorReadingX with temperature below the threshold.
     */
    public static class TemperatureFilter implements FilterFunction<SensorReadingX> {

        private final double threshold;

        public TemperatureFilter(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public boolean filter(SensorReadingX r) {
            return r.temperature >= threshold;
        }
    }

    /**
     * User-defined MapFunction to extract a reading's sensor id.
     */
    public static class IdExtractor implements MapFunction<SensorReadingX, String> {

        @Override
        public String map(SensorReadingX r) throws Exception {
            return r.id;
        }
    }

    /**
     * User-defined FlatMapFunction that splits a sensor's id String into a prefix and a number.
     */
    public static class IdSplitter implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String id, Collector<String> out) {

            String[] splits = id.split("_");

            for (String split : splits) {
                out.collect(split);
            }
        }
    }
}
