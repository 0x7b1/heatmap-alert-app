package ut.bigdata.heatmap.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TemperatureAlert {
    private String roomId;
    private Double alertedTemperature;

}
