package ut.bigdata.heatmap;

import ut.bigdata.heatmap.config.Config;

import static ut.bigdata.heatmap.config.Parameters.OFFSET;
import static ut.bigdata.heatmap.config.Parameters.KAFKA_HOST;
import static ut.bigdata.heatmap.config.Parameters.KAFKA_PORT;

import java.util.Properties;

public class KafkaUtils {
    public static Properties initConsumerProperties(Config config) {
        Properties kafkaProps = initProperties(config);
        String offset = config.get(OFFSET);
        kafkaProps.setProperty("auto.offset.reset", offset);
        return kafkaProps;
    }

    public static Properties initProducerProperties(Config params) {
        return initProperties(params);
    }

    private static Properties initProperties(Config config) {
        Properties kafkaProps = new Properties();
        String kafkaHost = config.get(KAFKA_HOST);
        int kafkaPort = config.get(KAFKA_PORT);
        String servers = String.format("%s:%s", kafkaHost, kafkaPort);
        kafkaProps.setProperty("bootstrap.servers", servers);
        return kafkaProps;
    }
}
