package ut.bigdata.heatmap.processors;

import jdk.internal.util.xml.impl.Input;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

@JsonSerialize
@Data
@AllArgsConstructor
@NoArgsConstructor
class InputMessage {
    String sender;
    String recipient;
    Long sentAt;
    String message;
}

class Backup {
    @JsonProperty("inputMessages")
    List<InputMessage> inputMessages;
    @JsonProperty("backupTimestamp")
    LocalDateTime backupTimestamp;
    @JsonProperty("uuid")
    UUID uuid;

    public Backup(List<InputMessage> inputMessages, LocalDateTime backupTimestamp) {
        this.inputMessages = inputMessages;
        this.backupTimestamp = backupTimestamp;
        this.uuid = UUID.randomUUID();
    }
}

public class StreamKafkaConsumer {
    public static class BackupAggregator implements AggregateFunction<InputMessage, List<InputMessage>, Backup> {
        @Override
        public List<InputMessage> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<InputMessage> add(InputMessage inputMessage, List<InputMessage> inputMessages) {
            inputMessages.add(inputMessage);
            return inputMessages;
        }

        @Override
        public Backup getResult(List<InputMessage> inputMessages) {
            return new Backup(inputMessages, LocalDateTime.now());
        }

        @Override
        public List<InputMessage> merge(List<InputMessage> inputMessages, List<InputMessage> acc1) {
            inputMessages.addAll(acc1);
            return inputMessages;
        }
    }

    public static class InputMessageTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<InputMessage> {
        public InputMessageTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(InputMessage inputMessage) {
            return inputMessage.getSentAt();
        }
    }

    public static class InputMessageTimestampAssigner implements AssignerWithPunctuatedWatermarks<InputMessage> {
        @Override
        public long extractTimestamp(InputMessage element, long previousElementTimestamp) {
            ZoneId zoneId = ZoneId.systemDefault();
            // EpochSecond is the format expected by Flink
//            return element.getSentAt().atZone(zoneId).toEpochSecond() * 1000; // 1000 converts sec to ms
            return element.getSentAt();
        }

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(InputMessage lastElement, long extractedTimestamp) {
            // Watermarks are useful in case of data that don't arrive in the order they were sent
            // A watermark defines the maximum lateness that is allowed for elements to be processed
            // Elements that have timestamps lower than the watermark won't be processed at all
            return new Watermark(extractedTimestamp - 1500);
        }
    }

    public static class BackupSerializationSchema implements SerializationSchema<Backup> {
        ObjectMapper objectMapper;
        Logger logger = LoggerFactory.getLogger(BackupSerializationSchema.class);

        @Override
        public byte[] serialize(Backup backupMessage) {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            try {
                return objectMapper.writeValueAsString(backupMessage).getBytes();
            } catch (JsonProcessingException e) {
                logger.error("Failed to parse JSON", e);
            }
            return new byte[0];
        }
    }

    public static class InputMessageDeserializationSchema implements DeserializationSchema<InputMessage> {
        static ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public InputMessage deserialize(byte[] bytes) throws IOException {
            return objectMapper.readValue(bytes, InputMessage.class);
        }

        @Override
        public boolean isEndOfStream(InputMessage inputMessage) {
            return false;
        }

        @Override
        public TypeInformation<InputMessage> getProducedType() {
            return TypeInformation.of(InputMessage.class);
        }
    }

    public static FlinkKafkaConsumer<String> createStringConsumerForTopic(String topic, String kafkaAddress, String kafkaGroup) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        // We should also provide a group id which will be used to hold offsets
        // so we won't always read the whole data from the beginning
        props.setProperty("group.id", kafkaGroup);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
            topic, new SimpleStringSchema(), props);

        return consumer;
    }

    public static FlinkKafkaProducer<String> createStringProducer(String topic, String kafkaAddress) {
        return new FlinkKafkaProducer<>(kafkaAddress, topic, new SimpleStringSchema());
    }

    public static void capitalize() throws Exception {
        String inputTopic = "test";
        String outputTopic = "test_out";
        String consumerGroup = "flink_consumer";
        String address = "localhost:29092";
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(
            inputTopic,
            address,
            consumerGroup);
        DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);

        FlinkKafkaProducer<String> flinkKafkaProducer = createStringProducer(
            outputTopic, address);

        stringInputStream
            .map(new MapFunction<String, String>() {
                @Override
                public String map(String s) throws Exception {
                    return s.toUpperCase();
                }
            })
            .addSink(flinkKafkaProducer);

        environment.execute();
    }

    public static void createBackup() throws Exception {
        String inputTopic = "test";
        String outputTopic = "test_out";
        String consumerGroup = "flink_consumer_2";
        String kafkaAddress = "localhost:29092";

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        FlinkKafkaConsumer<InputMessage> flinkKafkaConsumer = createInputMessageConsumer(inputTopic, kafkaAddress, consumerGroup);
        flinkKafkaConsumer.setStartFromEarliest();

//        flinkKafkaConsumer.assignTimestampsAndWatermarks(new InputMessageTimestampAssigner());
        flinkKafkaConsumer.assignTimestampsAndWatermarks(new InputMessageTimestampExtractor(Time.seconds(10)));
        FlinkKafkaProducer<Backup> flinkKafkaProducer = createBackupProducer(outputTopic, kafkaAddress);

        DataStream<InputMessage> inputMessagesStream
            = environment.addSource(flinkKafkaConsumer);

        inputMessagesStream
//            .timeWindowAll(Time.hours(24))
            .timeWindowAll(Time.seconds(5))
            .aggregate(new BackupAggregator())
            .addSink(flinkKafkaProducer);

        environment.execute();
    }

    public static void testCases() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // let's first create a stream simulating two events that are several minutes apart and
        // define a timestamp extractor that specifies our lateness threshold:
        SingleOutputStreamOperator<InputMessage> windowed = env.fromElements(
            new InputMessage("sender 1", "recp 1", ZonedDateTime.now().plusMinutes(25).toInstant().getEpochSecond(), "Hello 1"),
            new InputMessage("sender 1", "recp 2", ZonedDateTime.now().plusMinutes(5).toInstant().getEpochSecond(), "Hello 1"),
            new InputMessage("sender 1", "recp 3", ZonedDateTime.now().plusMinutes(0).toInstant().getEpochSecond(), "Hello 1"))
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<InputMessage>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(InputMessage element) {
                        return element.getSentAt() * 1000;
                    }
                });

        // Next, let's define a window operation to group our events into five-second
        // windows and apply a transformation on those events:
        SingleOutputStreamOperator<InputMessage> reduced = windowed
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .maxBy("sentAt");

        reduced.print();

//        KeyedStream
    }

    private static FlinkKafkaConsumer<InputMessage> createInputMessageConsumer(String topic, String kafkaAddress, String consumerGroup) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaAddress);
        props.setProperty("group.id", consumerGroup);

        FlinkKafkaConsumer<InputMessage> consumer =
            new FlinkKafkaConsumer<>(topic, new InputMessageDeserializationSchema(), props);

        return consumer;
    }

    private static FlinkKafkaProducer<Backup> createBackupProducer(String topic, String kafkaAddress) {
        return new FlinkKafkaProducer<>(kafkaAddress, topic, new BackupSerializationSchema());
    }

    public static void main(String[] args) throws Exception {
//        capitalize();
//        createBackup();
        testCases();
    }
}
