package ut.bigdata.heatmap.processors;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
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

    public SensorEventSource(SensorInOut sensorInOut, int numRooms, Double temperatureMean, Double temperatureStd) {
        this.sensorInOut = sensorInOut;
        this.numRooms = numRooms;
        this.temperatureMean = temperatureMean;
        this.temperatureStd = temperatureStd;
    }

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random rand = new Random();

        while (running) {
            Double temperature = rand.nextGaussian() * temperatureStd + temperatureMean;
            int roomId = rand.nextInt(numRooms) + 1;
            long currentTime = Calendar.getInstance().getTimeInMillis();

            SensorReading sensorReading = new SensorReading(roomId, currentTime, temperature, sensorInOut);

            ctx.collect(sensorReading);

            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
