package ut.bigdata;

import java.util.Objects;

public class TemperatureWarning {
    private Integer roomId;
    private Double avgTemperature;

    public TemperatureWarning(Integer roomId, Double avgTemperature) {
        this.roomId = roomId;
        this.avgTemperature = avgTemperature;
    }

    public TemperatureWarning() {
        this(-1, -1.0);
    }

    public Integer getRoomId() {
        return roomId;
    }

    public Double getAvgTemperature() {
        return avgTemperature;
    }

    @Override
    public String toString() {
        return "TemperatureWarning{" +
            "roomId=" + roomId +
            ", avgTemperature=" + avgTemperature +
            '}';
    }

    public void setRoomId(Integer roomId) {
        this.roomId = roomId;
    }

    public void setAvgTemperature(Double avgTemperature) {
        this.avgTemperature = avgTemperature;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TemperatureWarning that = (TemperatureWarning) o;
        return Objects.equals(roomId, that.roomId) &&
            Objects.equals(avgTemperature, that.avgTemperature);
    }

    @Override
    public int hashCode() {
        return 10 * super.hashCode() + Double.hashCode(avgTemperature);
    }
}
