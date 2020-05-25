import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import ut.bigdata.heatmap.model.Room;
import ut.bigdata.heatmap.model.Temperature;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import ut.bigdata.heatmap.serialization.RoomSerializer;
import ut.bigdata.heatmap.serialization.TemperatureSerializer;

import java.util.Map;
import java.util.Properties;

class Tuple {
    public Long t1;
    public Long t2;

    public Tuple(Long t1, Long t2) {
        this.t1 = t1;
        this.t2 = t2;
    }

    public Tuple() {
    }

    public void setT1(Long t1) {
        this.t1 = t1;
    }

    public void setT2(Long t2) {
        this.t2 = t2;
    }

    @Override
    public String toString() {
        return t1 + "-" + t2;
    }
}

class RoomDeserializer implements Deserializer<Room> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Room deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        Room user = null;
        try {
            user = mapper.readValue(data, Room.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return user;
    }

    @Override
    public void close() {

    }
}

class RoomSerde implements Serde<Room> {

    RoomSerializer serializer = new RoomSerializer();
    RoomDeserializer deserializer = new RoomDeserializer();

    @Override
    public Serializer<Room> serializer() {
        return (topic, data) -> serializer.serialize(topic, data);
    }

    @Override
    public Deserializer<Room> deserializer() {
        return (topic, data) -> deserializer.deserialize(topic, data);
    }
}

class TemperatureDeserializer implements Deserializer<Temperature> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Temperature deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        Temperature user = null;
        try {
            user = mapper.readValue(data, Temperature.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return user;
    }

    @Override
    public void close() {

    }
}

class TemperatureSerde implements Serde<Temperature> {

    TemperatureSerializer serializer = new TemperatureSerializer();
    TemperatureDeserializer deserializer = new TemperatureDeserializer();

    @Override
    public Serializer<Temperature> serializer() {
        return (topic, data) -> serializer.serialize(topic, data);
    }

    @Override
    public Deserializer<Temperature> deserializer() {
        return (topic, data) -> deserializer.deserialize(topic, data);
    }
}

class Tuple2Serde implements Serde<Tuple> {

    TupleSerializer serializer = new TupleSerializer();
    TupleDeserializer deserializer = new TupleDeserializer();

    @Override
    public Serializer<Tuple> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Tuple> deserializer() {
        return deserializer;
    }
}

class TupleDeserializer implements Deserializer<Tuple> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Tuple deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        Tuple t = null;
        try {
            t = mapper.readValue(data, Tuple.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return t;
    }

    @Override
    public void close() {

    }
}

class TupleSerializer implements Serializer<Tuple> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Tuple data) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(data).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    @Override
    public void close() {

    }
}

public class AverageStream {
    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rolling-average-kafkastream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, RoomSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TemperatureSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStream<Room, Temperature> measures =
            builder.stream("topic",
                Consumed.with(new RoomSerde(), new TemperatureSerde()));

        measures.mapValues((key, value) -> new Tuple(0L, (long) value.getValue()))
            .groupByKey().aggregate(() -> new Tuple(0L, 0L),
            (Room key, Tuple value, Tuple aggregate) -> {
                long t1 = aggregate.t1 + 1L;
                long t2 = ((long) aggregate.t2) + value.t2;
                return new Tuple(t1, t2);
            }, Materialized.with(new RoomSerde(),
                new Tuple2Serde()))
            .mapValues((readOnlyKey, value) -> value.t2 / value.t1)
            .toStream().print(Printed.toSysOut());


        Topology topology = builder.build();

        System.out.println(topology.describe());
        KafkaStreams ks = new KafkaStreams(topology, props);
        ks.start();
    }
}
