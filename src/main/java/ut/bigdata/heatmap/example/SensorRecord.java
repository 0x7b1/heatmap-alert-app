package ut.bigdata.heatmap.example;

import ut.bigdata.heatmap.SensorInOut;

import java.time.LocalDateTime;

public class SensorRecord {
    String id;
    LocalDateTime eventDate;
    Double temperature;
    SensorInOut inOut;

    public SensorRecord(String id, LocalDateTime eventDate, Double temperature, SensorInOut inOut) {
        this.id = id;
        this.eventDate = eventDate;
        this.temperature = temperature;
        this.inOut = inOut;
    }

    @Override
    public String toString() {
        return "SensorRecord{" +
            "id='" + id + '\'' +
            ", eventDate=" + eventDate +
            ", temperature=" + temperature +
            ", inOut=" + inOut +
            '}';
    }

    public String getId() {
        return id;
    }

    public LocalDateTime getEventDate() {
        return eventDate;
    }

    public Double getTemperature() {
        return temperature;
    }

    public SensorInOut getInOut() {
        return inOut;
    }
}
