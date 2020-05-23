package ut.bigdata.heatmap.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TemperatureRecord {
    String roomId;
    String source;
    Long timestamp;
    Integer temperature;
}
