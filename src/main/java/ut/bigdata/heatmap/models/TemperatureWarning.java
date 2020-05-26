package ut.bigdata.heatmap.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TemperatureWarning {
    private String roomId;
    private Double avgTemperature;
}
