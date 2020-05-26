package ut.bigdata.heatmap.transformations;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import ut.bigdata.heatmap.models.TemperatureRecord;

public class TemperatureRecordTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<TemperatureRecord> {
    public TemperatureRecordTimestampExtractor(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(TemperatureRecord temperatureRecord) {
        return temperatureRecord.getTimestamp();
    }
}
