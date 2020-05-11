package ut.bigdata;

import java.util.Objects;

public class SensorReading {
    private int roomId;
    private Long timestamp;
    private Double temperature;
    private SensorInOut inOut;

    public SensorReading() {
    }

    public SensorReading(Long timestamp, Double temperature, Integer roomId, SensorInOut inOut) {
        this.timestamp = timestamp;
        this.temperature = temperature;
        this.roomId = roomId;
        this.inOut = inOut;
    }

    public void setRoomId(int roomId) {
        this.roomId = roomId;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
            ", timestamp=" + timestamp +
            ", temperature=" + temperature +
            ", roomId='" + roomId + '\'' +
            ", inOut=" + inOut +
            '}';
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public void setInOut(SensorInOut inOut) {
        this.inOut = inOut;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public Integer getRoomId() {
        return roomId;
    }

    public SensorInOut getInOut() {
        return inOut;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SensorReading that = (SensorReading) o;
        return Objects.equals(timestamp, that.timestamp) &&
            Objects.equals(temperature, that.temperature) &&
            Objects.equals(roomId, that.roomId) &&
            inOut == that.inOut;
    }

    @Override
    public int hashCode() {
        return 10 * super.hashCode() + Double.hashCode(temperature);
    }
}
