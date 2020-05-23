package ut.bigdata.heatmap.processors;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ut.bigdata.heatmap.functions.TimestampAssignable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading implements TimestampAssignable<Long> {
    private long roomId;
    private long timestamp;
    private double temperature;
    private SensorInOut inOut;

    @Override
    public void assignIngestionTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
