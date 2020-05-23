package ut.bigdata.heatmap.processors;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Alert<Event, Value> {
    private Integer ruleId;
    private String key;

    private Event triggeringEvent;
    private Value triggeringValue;
}
