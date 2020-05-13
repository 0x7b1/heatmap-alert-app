package ut.bigdata.heatmap;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import ut.bigdata.ref.SensorRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class HeatMapBatch {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        DataSet<Tuple4<String, String, Double, String>> dataset = env.readCsvFile("dataset/IOT-temp.csv")
            //.ignoreFirstLine()
            .ignoreInvalidLines()
            .includeFields(true, false, true, true, true)
            .types(String.class, String.class, Double.class, String.class);

        DataSet<SensorRecord> records = dataset.map(new MapFunction<Tuple4<String, String, Double, String>, SensorRecord>() {
            @Override
            public SensorRecord map(Tuple4<String, String, Double, String> csvLine) throws Exception {
                String id = csvLine.f0;
                LocalDateTime eventDate = LocalDateTime.parse(csvLine.f1, DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"));
                Double temperature = csvLine.f2;
                SensorInOut inOut = csvLine.f3.equals("In") ? SensorInOut.IN : SensorInOut.OUT;

                return new SensorRecord(id, eventDate, temperature, inOut);
            }
        });

        DataSet<SensorRecord> recordsIn = records.filter(new FilterFunction<SensorRecord>() {
            @Override
            public boolean filter(SensorRecord record) throws Exception {
                return record.getInOut().equals(SensorInOut.IN);
            }
        });

        DataSet<SensorRecord> recordsOut = records.filter(new FilterFunction<SensorRecord>() {
            @Override
            public boolean filter(SensorRecord record) throws Exception {
                return record.getInOut().equals(SensorInOut.OUT);
            }
        });

        DataSet<SensorRecord> recordsTemp40OrMore = records.filter(new FilterFunction<SensorRecord>() {
            @Override
            public boolean filter(SensorRecord record) throws Exception {
                return record.getTemperature() >= 40;
            }
        });

        DataSet<SensorRecord> recordsInTemp40 = records.filter(new FilterFunction<SensorRecord>() {
            @Override
            public boolean filter(SensorRecord record) throws Exception {
                return record.getInOut().equals(SensorInOut.IN) && record.getTemperature() >= 40;
            }
        });

        DataSet<SensorRecord> recordsOutTemp40 = records.filter(new FilterFunction<SensorRecord>() {
            @Override
            public boolean filter(SensorRecord record) throws Exception {
                return record.getInOut().equals(SensorInOut.OUT) && record.getTemperature() >= 40;
            }
        });

        //records.print();

        //System.out.println(String.format("Records IN = %s; OUT = %s", recordsIn.count(), recordsOut.count()));
        //System.out.println(String.format("Records 40 or more = %s", recordsTemp40OrMore.count()));
        //System.out.println(String.format("Records IN = %s, of >=40 Temp = %s; OUT = %s, of >=40 Temp = %s", recordsIn.count(), recordsInTemp40.count(), recordsOut.count(), recordsOutTemp40.count()));
        /*
        * Conclusion one of the previous testers reached
        * Inside(Outside) = 0.89 * Outside+3 if Outside between 25,35
        * otherwise with higher outside values the inside in 28,35 (more erratic?)
        * */

        //Sort the partition to iterate through it
        //DataSet<SensorRecord> recSorted = records.sortPartition(SensorRecord::getEventDate, Order.ASCENDING);
        //recSorted.print();

        //Note collecting appears to ruin SortPartition, needs further checking
        List<SensorRecord> resultsIn = recordsIn.collect();
        resultsIn.sort(new Comparator<SensorRecord>() {
            @Override
            public int compare(SensorRecord o1, SensorRecord o2) {
                return o2.getEventDate().compareTo(o1.getEventDate());
            }
        });

        int oldest = 0;
        int latest = 0;
        List<Integer> nums = new ArrayList<>();
        boolean oldSwap = false;
        int tempLimit = 35;
        for(int i = resultsIn.size()-1; i >= 0; i--) {
            SensorRecord rec = resultsIn.get(i);
            if (rec.getTemperature() >= tempLimit && !oldSwap) {
                oldSwap = true;
                oldest = i;
            }
            else if (rec.getTemperature() >= tempLimit && oldSwap){
                continue;
            }
            else {
                oldSwap = false;
                SensorRecord oldRec = resultsIn.get(oldest);

                //Ignores seconds for now
                if (rec.getEventDate().getHour() > oldRec.getEventDate().getHour()
                    && rec.getEventDate().getMinute() >= oldRec.getEventDate().getMinute()) {

                    for(int j = oldest; j < latest; j++)
                        nums.add(j);
                }
            }
            latest = i;
        }
        /*System.out.println(results.get(0));
        System.out.println(results.get(results.size()-1));
        System.out.println(results.size());*/
        System.out.println(nums);

    }
}
