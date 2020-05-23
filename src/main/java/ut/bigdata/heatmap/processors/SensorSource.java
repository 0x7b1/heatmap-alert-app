package ut.bigdata.heatmap.processors;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class SensorSource extends RichParallelSourceFunction<SensorReading> {
    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random rand = new Random();
        int indexTask = this.getRuntimeContext().getIndexOfThisSubtask();

        String[] sensorIds = new String[10];
        double[] currentTemp = new double[10];

        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "sensor_" + (indexTask * 10 + i);
            currentTemp[i] = 65 + (rand.nextGaussian() * 20);
        }

        while (running) {
            long currentTime = Calendar.getInstance().getTimeInMillis();

            for (int i = 0; i < 10; i++){
                currentTemp[i] += rand.nextGaussian() * 0.5;
//                ctx.collect(new SensorReading(sensorIds[i], currentTime, currentTemp[i], ""));
            }

            Thread.sleep(1200);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
