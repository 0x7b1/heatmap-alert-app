package ut.bigdata.heatmap.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class RollingSum {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        DataStream<Tuple3<Integer, Integer, Integer>> triplets = env.fromElements(
//            Tuple3.of(10, 1, 1),
//            Tuple3.of(11, 2, 2),
//            Tuple3.of(10, 1, 3),
//            Tuple3.of(11, 2, 4));
//
//        triplets
//            .keyBy(0)
//            .sum(1)
//            .print();

        DataStream<String> text = env.fromElements(
            "hello world",
            "maria db",
            "hello amigos",
            "amigos db"
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> words = text
            // Rebalance causes the data to be repartitioned, so all machines receive messages
            // This applies when the number of kafka partitions is fewer than the number of flink parallel instances
//            .rebalance()
            .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                    String[] words = s.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                }
            });

        KeyedStream<Tuple2<String, Integer>, String> groupWords = words.keyBy(e -> e.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount =  groupWords.sum(1);

        wordCount.print();

        env.execute();
    }
}
