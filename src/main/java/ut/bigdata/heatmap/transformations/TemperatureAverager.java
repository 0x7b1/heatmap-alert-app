package ut.bigdata.heatmap.transformations;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import ut.bigdata.heatmap.models.TemperatureRecord;


public class TemperatureAverager implements WindowFunction<TemperatureRecord, Tuple3<String, Double, Long>, String, TimeWindow> {
    @Override
    public void apply(
        String roomId,
        TimeWindow timeWindow,
        Iterable<TemperatureRecord> recordsWithinWindows,
        Collector<Tuple3<String, Double, Long>> out) throws Exception {
        int sum = 0;
        int count = 0;

        for (TemperatureRecord record : recordsWithinWindows) {
            sum += record.getTemperature();
            count += 1;
        }

        Double avgWindowBySource = sum * 1.0 / count;
        Long windowTimestamp = timeWindow.getEnd();

        out.collect(Tuple3.of(roomId, avgWindowBySource, windowTimestamp));
    }
}
