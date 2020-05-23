package ut.bigdata.heatmap.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Properties;

public class SimpleKafkaConsumer {
    @JsonSerialize
    public class InputMessage {
        String sender;
        String recipient;
        LocalDateTime sentAt;
        String message;
    }

    public static void main(String[] args) throws Exception {
        String inputTopic = "test";
        String outputTopic = "test_out";
        String consumerGroup = "console-consumer-67781";
        String address = "localhost:29092";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, address, consumerGroup);
        DataStream<String> stringStream = env.addSource(flinkKafkaConsumer);

        FlinkKafkaProducer<String> flinkKafkaProducer = createStringProducer(outputTopic, address);

        stringStream
            .map(new WordsCapitalizer())
            .addSink(flinkKafkaProducer);
//            .print();

        env.execute();
    }

    public static FlinkKafkaConsumer<String> createStringConsumerForTopic(
        String topic, String kafkaAddress, String kafkaGroup) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        props.setProperty("group.id", kafkaGroup);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
            topic, new SimpleStringSchema(), props);

        return consumer;
    }

    public static FlinkKafkaProducer<String> createStringProducer(
        String topic, String kafkaAddress) {

        return new FlinkKafkaProducer<>(kafkaAddress,
            topic, new SimpleStringSchema());
    }

    public static class WordsCapitalizer implements MapFunction<String, String> {
        @Override
        public String map(String s) throws Exception {
            return s.toUpperCase();
        }
    }
}
