package ut.bigdata;

public class SensorReading {
    public String id;
    public long timestamp;
    public double temperature;
    public String sensorSource; // integrate

    public SensorReading() {
    }

    public SensorReading(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
            "id='" + id + '\'' +
            ", timestamp=" + timestamp +
            ", temperature=" + temperature +
            '}';
    }
}
