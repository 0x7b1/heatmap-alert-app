package ut.bigdata.heatmap.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;

import java.util.HashMap;

public class TemperatureAvgRoomsToInfluxDataPoint implements MapFunction<Tuple3<String, Double, Long>, InfluxDBPoint> {
    String measurement;

    public TemperatureAvgRoomsToInfluxDataPoint(String measurementName) {
        this.measurement = measurementName;
    }

    @Override
    public InfluxDBPoint map(Tuple3<String, Double, Long> record) throws Exception {
        String measurement = this.measurement;
        Long timestamp = record.f2;
        HashMap<String, String> tags = new HashMap<>();
        HashMap<String, Object> fields = new HashMap<>();

        tags.put("room", record.f0);

        fields.put("value", record.f1);

        return new InfluxDBPoint(measurement, timestamp, tags, fields);
    }
}
