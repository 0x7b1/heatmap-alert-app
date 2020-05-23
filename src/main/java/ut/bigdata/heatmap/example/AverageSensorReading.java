package ut.bigdata.heatmap.example;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Random;

public class AverageSensorReading {
    public static void simpleReading() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStreamSink<Tuple2<String, Integer>> sensorReadings = env
            .addSource(new SourceFunction<Tuple2<String, Integer>>() {
                boolean running = true;

                @Override
                public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                    ctx.collect(Tuple2.of("room_2_out", 20));
                    ctx.collect(Tuple2.of("room_2_out", 40));
                    ctx.collect(Tuple2.of("room_1_in", 21));
                    ctx.collect(Tuple2.of("room_2_in", 22));
                }

                @Override
                public void cancel() {
                    running = false;
                }
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Integer>>(Time.seconds(10L)) {
                @Override
                public long extractTimestamp(Tuple2<String, Integer> record) {
                    return record.f1 * 1000;
                }
            })
            .keyBy(0)
            .timeWindow(Time.seconds(5))
            .sum(1)
            .print();

        env.execute();
    }

    public static void testWithTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSink<Tuple3<String, Integer, Long>> windowed = env.fromElements(
            Tuple3.of("room_16", 5, ZonedDateTime.now().plusSeconds(1).toInstant().getEpochSecond()),
            Tuple3.of("room_16", 5, ZonedDateTime.now().plusSeconds(2).toInstant().getEpochSecond()),
            Tuple3.of("room_16", 5, ZonedDateTime.now().plusSeconds(5).toInstant().getEpochSecond()),
            Tuple3.of("room_16", 5, ZonedDateTime.now().plusSeconds(9).toInstant().getEpochSecond()),
            Tuple3.of("room_16", 5, ZonedDateTime.now().plusSeconds(15).toInstant().getEpochSecond()),
            Tuple3.of("room_14", 5, ZonedDateTime.now().plusSeconds(1).toInstant().getEpochSecond()),
            Tuple3.of("room_14", 5, ZonedDateTime.now().plusSeconds(3).toInstant().getEpochSecond()),
            Tuple3.of("room_14", 5, ZonedDateTime.now().plusSeconds(15).toInstant().getEpochSecond()),
            Tuple3.of("room_15", 5, ZonedDateTime.now().plusSeconds(4).toInstant().getEpochSecond()))
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, Long>>(Time.seconds(5)) {
                @Override
                public long extractTimestamp(Tuple3<String, Integer, Long> record) {
                    return record.f1 * 1000;
                }
            })
            .keyBy(e -> e.f0)
//            .window(TumblingEventTimeWindows.of(Time.seconds(1)))
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
//            .timeWindow(Time.seconds(1))
            .sum(1)
            .print();
//            .maxBy(0 ,true)
//            .print();


        env.execute();

    }

    public static void testWithTimeAndKafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


    }

    public static void testWithTimeAndLocalSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointInterval(10_000);
        env.setParallelism(1);

        DataStream<Tuple3<String, Integer, String>> inAvgs = env.addSource(new TemperatureSource("IN"))
            .assignTimestampsAndWatermarks(new TemperatureWatermark(Time.seconds(0))).keyBy(e -> e.f0)
            .timeWindow(Time.seconds(1))
            .apply(new TemperatureAverager());

        DataStream<Tuple4<String, String, Integer, String>> inAvgTransformed = inAvgs
            .map(new MapAverageIntoRooms());


        DataStream<Tuple3<String, Integer, String>> outAvgs = env.addSource(new TemperatureSource("OUT"))
            .assignTimestampsAndWatermarks(new TemperatureWatermark(Time.seconds(0))).keyBy(e -> e.f0)
            .timeWindow(Time.seconds(1))
            .apply(new TemperatureAverager());

        DataStream<Tuple4<String, String, Integer, String>> outAvgTransformed = outAvgs
            .map(new MapAverageIntoRooms());

        inAvgTransformed
            .join(outAvgTransformed)
            .where(in -> {
//                System.out.println("in---->" + in);
                return in.f0;
            })
            .equalTo(out -> {
//                System.out.println("out---->" + out);
                return out.f0;
            })
            .window(TumblingEventTimeWindows.of(Time.seconds(1)))
            .apply(new JoinFunction<Tuple4<String, String, Integer, String>, Tuple4<String, String, Integer, String>, String>() {
                @Override
                public String join(
                    Tuple4<String, String, Integer, String> rIn,
                    Tuple4<String, String, Integer, String> rOut) throws Exception {
//                    System.out.println("RIN------ " + rIn);
//                    System.out.println("ROUT------ " + rOut);

                    Double relation = rIn.f2 * 1.0 / rOut.f2;
                    return relation.toString();
                }
            })
            .print();
        System.out.println(env.getExecutionPlan());
//        inAvgs
//            .connect(outAvgs.broadcast())
//            .connect(outAvgs)
//            .keyBy(0, 0)
//            .flatMap(new CoFlatMapFunction<Tuple3<String, Integer, String>, Tuple3<String, Integer, String>, String>() {
//                @Override
//                public void flatMap1(Tuple3<String, Integer, String> stringIntegerStringTuple3, Collector<String> collector) throws Exception {
//
//                }
//
//                @Override
//                public void flatMap2(Tuple3<String, Integer, String> stringIntegerStringTuple3, Collector<String> collector) throws Exception {
//
//                }
//            })

//        inAvgs
//            .join(outAvgs)
//            .where(a -> {
//                System.out.println("a--->" + a);
//                return a.f1;
//            })
//            .equalTo(b -> {
//                System.out.println("b--->" + b);
//                return b.f1;
//            })
//            .window(TumblingEventTimeWindows.of(Time.seconds(1)))
//            .apply(new JoinFunction<Tuple3<String, Integer, String>, Tuple3<String, Integer, String>, String>() {
//                @Override
//                public String join(
//                    Tuple3<String, Integer, String> recordIn,
//                    Tuple3<String, Integer, String> recordOut) throws Exception {
//                    System.out.println("IN->" + recordIn);
//                    System.out.println("OUT->" + recordOut);
//                    return "Hello";
//                }
//            });


//            .print();
//        inAvgs.union(outAvgs).print();

//        inAvgs.print();
//        outAvgs.print();

        env.execute();
    }

    public static class TemperatureWatermark extends BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, Long>> {
        public TemperatureWatermark(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple3<String, Integer, Long> record) {
            return record.f2;
        }
    }

    public static class TemperatureAverager implements WindowFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, String>, String, TimeWindow> {
        @Override
        public void apply(
            String key,
            TimeWindow timeWindow,
            Iterable<Tuple3<String, Integer, Long>> iterable,
            Collector<Tuple3<String, Integer, String>> collector) throws Exception {
            Integer acc = 0;
            int count = 0;
            StringBuilder nrs = new StringBuilder();

            for (Tuple3<String, Integer, Long> record : iterable) {
//                System.out.println("[+] " + record.f0 + ": " + record.f1);
                nrs.append(record.f1).append(", ");
                acc += record.f1;
                count += 1;
            }

            double avg = acc / count;

            collector.collect(Tuple3.of(key, (int) avg, nrs.toString()));
        }
    }

    public static class TemperatureSource implements SourceFunction<Tuple3<String, Integer, Long>> {
        boolean running = true;
        String source;

        public TemperatureSource(String source) {
            this.source = source;
        }

        @Override
        public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
            Random rand = new Random();
            while (running) {
                String roomId = "room_" + rand.nextInt(1) + "_" + source;
                Long timestamp = ZonedDateTime.now().toEpochSecond() * 1000;
                Integer temperature = (int) (rand.nextGaussian() * 10);
                ctx.collect(Tuple3.of(roomId, temperature, timestamp));
                Thread.sleep(200);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class MapAverageIntoRooms implements MapFunction<Tuple3<String, Integer, String>, Tuple4<String, String, Integer, String>> {
        @Override
        public Tuple4<String, String, Integer, String> map(Tuple3<String, Integer, String> rec) throws Exception {
            String[] roomIdVars = rec.f0.split("_");

            String source = roomIdVars[2];
            String roomId = roomIdVars[0] + "_" + roomIdVars[1]; // Discarding the source
            return Tuple4.of(roomId, source, rec.f1, rec.f2);
        }
    }

    public static void main(String[] args) throws Exception {
//        simpleReading();
//        testWithTime();
//        testWithTimeAndKafka();
        testWithTimeAndLocalSource();
    }
}
