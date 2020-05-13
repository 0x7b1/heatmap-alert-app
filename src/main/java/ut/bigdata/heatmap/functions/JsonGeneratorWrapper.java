package ut.bigdata.heatmap.functions;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.SplittableRandom;

public class JsonGeneratorWrapper<T> extends BaseGenerator<String> {

  private BaseGenerator<T> wrappedGenerator;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public JsonGeneratorWrapper(BaseGenerator<T> wrappedGenerator) {
    this.wrappedGenerator = wrappedGenerator;
    this.maxRecordsPerSecond = wrappedGenerator.getMaxRecordsPerSecond();
  }

  @Override
  public String randomEvent(SplittableRandom rnd, long id) {
    T event = wrappedGenerator.randomEvent(rnd, id);

    String json;
    try {
      json = objectMapper.writeValueAsString(event);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return json;
  }
}
