package ut.bigdata.heatmap.transformations;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class JoinWindowsToTemperatureRelation implements JoinFunction<Tuple3<String, Double, Long>, Tuple3<String, Double, Long>, Tuple3<String, Double, Long>> {
    @Override
    public Tuple3<String, Double, Long> join(
        Tuple3<String, Double, Long> avgIn,
        Tuple3<String, Double, Long> avgOut) throws Exception {

        String roomId = avgIn.f0;
        Double indexRelation = avgIn.f1 / avgOut.f1;
        Long windowTimestamp = avgIn.f2;

        return Tuple3.of(roomId, indexRelation, windowTimestamp);
    }
}
