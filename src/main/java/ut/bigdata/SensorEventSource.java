package ut.bigdata;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Objects;
import java.util.Random;

public class SensorEventSource extends RichParallelSourceFunction<SensorReading> {
    private boolean running = true;
    private final SensorInOut sensorInOut;
    private final int numRooms;
    private final Double temperatureStd;
    private final Double temperatureMean;

//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//    }

    public SensorEventSource(SensorInOut sensorInOut, int numRooms) {
        this.sensorInOut = sensorInOut;
        this.numRooms = numRooms;
        this.temperatureStd = 10.0;
        this.temperatureMean = 100.0;
    }

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random rand = new Random();

        while (running) {
            Double temperature = rand.nextGaussian() * temperatureMean * temperatureStd;
            int roomId = rand.nextInt(numRooms) + 1;
            long currentTime = Calendar.getInstance().getTimeInMillis();

            SensorReading sensorReading = new SensorReading(currentTime, temperature, roomId, sensorInOut);

            ctx.collect(sensorReading);

            Thread.sleep(500);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SensorEventSource that = (SensorEventSource) o;
        return running == that.running &&
            numRooms == that.numRooms &&
            sensorInOut == that.sensorInOut &&
            Objects.equals(temperatureStd, that.temperatureStd) &&
            Objects.equals(temperatureMean, that.temperatureMean);
    }

    @Override
    public int hashCode() {
        return Objects.hash(running, sensorInOut, numRooms, temperatureStd, temperatureMean);
    }

    @Override
    public void cancel() {
        running = false;
    }
}
