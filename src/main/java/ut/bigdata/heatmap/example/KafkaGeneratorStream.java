package ut.bigdata.heatmap.example;

import java.io.Serializable;
import java.time.LocalTime;
import java.util.Properties;

import lombok.Data;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Data
class KafkaRecord implements Serializable {
    String key;
    String value;
    Long timestamp;

    @Override
    public String toString() {
        return key + ":" + value;
    }

}

class Producer<T> {
    String bootstrapServers;
    KafkaProducer<String, T> producer;

    public Producer(String kafkaServer, String serializerName) {
        this.bootstrapServers = kafkaServer;
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerName);

        // create the producer
        producer = new KafkaProducer<String, T>(properties);
    }

    public void send(String topic, T message) {
        // create a producer record
        ProducerRecord<String, T> record = new ProducerRecord<String, T>(topic, "myKey", message);

        // send data - asynchronous
        producer.send(record);

        // flush data
        producer.flush();
    }

    public void close() {
        // flush and close producer
        producer.close();
    }
}

class MySchema implements KafkaDeserializationSchema<KafkaRecord> {
    @Override
    public boolean isEndOfStream(KafkaRecord nextElement) {
        return false;
    }

    @Override
    public KafkaRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        KafkaRecord data = new KafkaRecord();
        data.key = new String(record.key());
        data.value = new String(record.value());
        data.timestamp = record.timestamp();

        return data;
    }

    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
        return TypeInformation.of(KafkaRecord.class);
    }
}

class NumberGenerator extends Thread {
    int counter = 0;
    final Producer<String> p;
    final String topic;

    public NumberGenerator(Producer<String> p, String topic) {
        this.p = p;
        this.topic = topic;
    }

    @Override
    public void run() {
        try {
            while (++counter > 0) {
                p.send(topic, "[" + counter + "]");

                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}


public class KafkaGeneratorStream {

    public static void main1() throws Exception {
        String TOPIC_IN = "test";
        String BOOTSTRAP_SERVER = "localhost:29092";
        Producer<String> p = new Producer<String>(BOOTSTRAP_SERVER, StringSerializer.class.getName());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("client.id", "flink-example1");

        // Reading data directly as <Key, Value> from Kafka. Write an inner class containing key, value
        // and use it to deserialise Kafka record.
        // Reference => https://stackoverflow.com/questions/53324676/how-to-use-flinkkafkaconsumer-to-parse-key-separately-k-v-instead-of-t
        FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(TOPIC_IN, new MySchema(), props);

        kafkaConsumer.setStartFromLatest();

        // create a stream to ingest data from Kafka as a custom class with explicit key/value
        DataStream<KafkaRecord> stream = env.addSource(kafkaConsumer);

        // supports timewindow without group by key
        stream
            .timeWindowAll(Time.seconds(5))
            .reduce(new ReduceFunction<KafkaRecord>() {
                KafkaRecord result = new KafkaRecord();

                @Override
                public KafkaRecord reduce(KafkaRecord record1, KafkaRecord record2) throws Exception {
                    System.out.println(LocalTime.now() + " -> " + record1 + "   " + record2);

                    result.key = record1.key;
                    result.value = record1.value + record2.value;

                    return result;
                }
            })
            .print(); // immediate printing to console

        //.keyBy( (KeySelector<KafkaRecord, String>) KafkaRecord::getKey )
        //.timeWindow(Time.seconds(5))

        // produce a number as string every second
        new NumberGenerator(p, TOPIC_IN).start();

        // for visual topology of the pipeline. Paste the below output in https://flink.apache.org/visualizer/
        System.out.println(env.getExecutionPlan());

        // start flink
        env.execute();
    }

    public static void main2() throws Exception {
        String TOPIC_IN = "Topic2-IN";
        String TOPIC_OUT = "Topic2-OUT";
        String BOOTSTRAP_SERVER = "localhost:9092";

        Producer<String> p = new Producer<String>(BOOTSTRAP_SERVER, StringSerializer.class.getName());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("client.id", "flink-example2");

        // Alternate consumer to get only values per Topic
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(TOPIC_IN, new SimpleStringSchema(), props);
        kafkaConsumer.setStartFromLatest();

        // Create Kafka producer from Flink API
        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", BOOTSTRAP_SERVER);

        FlinkKafkaProducer<String> kafkaProducer =
            new FlinkKafkaProducer<String>(TOPIC_OUT,
                ((value, timestamp) -> new ProducerRecord<byte[], byte[]>(TOPIC_OUT, "myKey".getBytes(), value.getBytes())),
                prodProps,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        // create a stream to ingest data from Kafka with value as String
        DataStream<String> stream = env.addSource(kafkaConsumer);

        stream
            .timeWindowAll(Time.seconds(5)) // ignoring grouping per key
            .reduce(new ReduceFunction<String>() {
                @Override
                public String reduce(String value1, String value2) throws Exception {
                    System.out.println(LocalTime.now() + " -> " + value1 + "   " + value2);
                    return value1 + value2;
                }
            })
            .addSink(kafkaProducer);

        // produce a number as string every second
        new NumberGenerator(p, TOPIC_IN).start();

        // for visual topology of the pipeline. Paste the below output in https://flink.apache.org/visualizer/
        System.out.println(env.getExecutionPlan());

        // start flink
        env.execute();
    }

    public static void main3() throws Exception {
        String TOPIC_IN = "Topic3-IN";
        String TOPIC_OUT = "Topic3-OUT";
        String BOOTSTRAP_SERVER = "localhost:9092";

        Producer<String> p = new Producer<String>(BOOTSTRAP_SERVER, StringSerializer.class.getName());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // to use allowed lateness
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("client.id", "flink-example3");

        // consumer to get both key/values per Topic
        FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(TOPIC_IN, new MySchema(), props);

        // for allowing Flink to handle late elements
        kafkaConsumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<KafkaRecord>() {
            @Override
            public long extractAscendingTimestamp(KafkaRecord record) {
                return record.timestamp;
            }
        });

        kafkaConsumer.setStartFromLatest();

        // Create Kafka producer from Flink API
        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", BOOTSTRAP_SERVER);

        FlinkKafkaProducer<String> kafkaProducer =
            new FlinkKafkaProducer<String>(TOPIC_OUT,
                ((value, timestamp) -> new ProducerRecord<byte[], byte[]>(TOPIC_OUT, "myKey".getBytes(), value.getBytes())),
                prodProps,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        // create a stream to ingest data from Kafka with key/value
        DataStream<KafkaRecord> stream = env.addSource(kafkaConsumer);

        stream
            .filter((record) -> record.value != null && !record.value.isEmpty())
            .keyBy(record -> record.key)
            .timeWindow(Time.seconds(5))
            .allowedLateness(Time.milliseconds(500))
            .aggregate(new AggregateFunction<KafkaRecord, String, String>()  // kafka aggregate API is very simple but same can be achieved by Flink's reduce
            {
                @Override
                public String createAccumulator() {
                    return "";
                }

                @Override
                public String add(KafkaRecord record, String accumulator) {
                    return accumulator + record.value.length();
                }

                @Override
                public String getResult(String accumulator) {
                    return accumulator;
                }

                @Override
                public String merge(String a, String b) {
                    return a + b;
                }
            })
            .addSink(kafkaProducer);

        // produce a number as string every second
        new NumberGenerator(p, TOPIC_IN).start();

        // for visual topology of the pipeline. Paste the below output in https://flink.apache.org/visualizer/
        System.out.println(env.getExecutionPlan());

        // start flink
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        main1();
//        main2();
//        main3();
    }
}
