package ut.bigdata;

import java.util.Objects;

public class TemperatureAlert {
    private Integer roomId;

    public TemperatureAlert(Integer roomId) {
        this.roomId = roomId;
    }

    @Override
    public String toString() {
        return "TemperatureAlert{" +
            "roomId=" + roomId +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TemperatureAlert that = (TemperatureAlert) o;
        return Objects.equals(roomId, that.roomId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roomId);
    }
}
