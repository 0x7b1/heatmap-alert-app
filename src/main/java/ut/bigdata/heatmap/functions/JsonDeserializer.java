package ut.bigdata.heatmap.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class JsonDeserializer<T> extends RichFlatMapFunction<String, T> {

    private JsonMapper<T> parser;
    private final Class<T> targetClass;

    public JsonDeserializer(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parser = new JsonMapper<>(targetClass);
    }

    @Override
    public void flatMap(String value, Collector<T> out) throws Exception {
        try {
            T parsed = parser.fromString(value);
            out.collect(parsed);
        } catch (Exception e) {
            System.out.println("Error parsing " + e);
        }
    }
}
