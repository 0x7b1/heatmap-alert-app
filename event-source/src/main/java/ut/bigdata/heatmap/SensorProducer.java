package ut.bigdata.heatmap;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import ut.bigdata.heatmap.model.Room;
import ut.bigdata.heatmap.model.Temperature;
import ut.bigdata.heatmap.partitioner.RoomPartitioner;
import ut.bigdata.heatmap.serialization.RoomSerializer;
import ut.bigdata.heatmap.serialization.TemperatureSerializer;

import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.Random;

public class SensorProducer {
    static String topicName = "sensor_temperatures";
    static String kafkaServer = "kafka:9092";
    static int numRooms = 2;

    public static ProducerRecord<Room, Temperature> createTemperatureRecord(String source) {
        Random rand = new Random();
        String roomId = rand.nextInt(numRooms) + "_" + source;
        Room key = new Room(roomId);

//        long timestamp = ZonedDateTime.now().toEpochSecond() * 1000;
        long timestamp = System.currentTimeMillis();
        int temperatureValue = rand.nextInt(20) + 20;
        Temperature value = new Temperature(temperatureValue, timestamp);

        ProducerRecord<Room, Temperature> record = new ProducerRecord<>(topicName, key, value);

        return record;
    }

    public static KafkaProducer<Room, Temperature> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, RoomSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TemperatureSerializer.class);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoomPartitioner.class);

        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaProducer<Room, Temperature> producer = createKafkaProducer();

        while (true) {
            producer.send(createTemperatureRecord("IN"));
            producer.send(createTemperatureRecord("OUT"));

            Thread.sleep(500);
        }
    }
}
