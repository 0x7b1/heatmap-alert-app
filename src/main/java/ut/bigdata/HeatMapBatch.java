package ut.bigdata;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class HeatMapBatch {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        DataSet<Tuple4<String, String, Double, String>> dataset = env.readCsvFile("dataset/IOT-temp.csv")
            .ignoreFirstLine()
            .ignoreInvalidLines()
            .includeFields(true, false, true, true, true)
            .types(String.class, String.class, Double.class, String.class);

        DataSet<SensorRecord> records = dataset.map(new MapFunction<Tuple4<String, String, Double, String>, SensorRecord>() {
            @Override
            public SensorRecord map(Tuple4<String, String, Double, String> csvLine) throws Exception {
                String id = csvLine.f0;
                LocalDateTime eventDate = LocalDateTime.parse(csvLine.f1, DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm"));
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

//        records.print();

        System.out.println(String.format("Records IN = %s; OUT = %s", recordsIn.count(), recordsOut.count()));
    }
}
