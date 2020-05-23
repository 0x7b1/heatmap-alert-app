package ut.bigdata.heatmap.functions;

import ut.bigdata.heatmap.processors.SensorInOut;
import ut.bigdata.heatmap.processors.SensorReading;

import java.util.Calendar;
import java.util.Random;
import java.util.SplittableRandom;

public class TemperatureGenerator extends BaseGenerator<SensorReading> {
    private static double TEMPERATURE_STD = 10d;
    private static double TEMPERATURE_MEAN = 10d;
    private static long MAX_ROOMS = 10;

    public TemperatureGenerator(int maxRecordsPerSecond) {
        super(maxRecordsPerSecond);
    }

    @Override
    public SensorReading randomEvent(SplittableRandom rnd, long id) {
        Random rand = new Random();

        double temperature = rand.nextGaussian() * TEMPERATURE_STD + TEMPERATURE_MEAN;
        long roomId = rnd.nextLong(MAX_ROOMS);
        long currentTime = Calendar.getInstance().getTimeInMillis();

        return SensorReading.builder()
            .roomId(roomId)
            .timestamp(currentTime)
            .temperature(temperature)
            .inOut(getSensorInOut(id))
            .build();
    }

    private SensorInOut getSensorInOut(long id) {
        int name = (int) (id % 2);
        switch (name) {
            case 0:
                return SensorInOut.IN;
            case 1:
                return SensorInOut.OUT;
            default:
                throw new IllegalStateException("");
        }
    }
}
