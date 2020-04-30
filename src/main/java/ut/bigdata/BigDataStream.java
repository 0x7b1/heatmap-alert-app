package ut.bigdata;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/heatmap-alert-app-1.0-SNAPSHOT.jar
 * From the CLI you can then run
 * 		./bin/flink run -c ut.bigdata.ref.StreamingJob target/heatmap-alert-app-1.0-SNAPSHOT.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class BigDataStream {

    public static void main(String[] args) throws Exception {
        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.readTextFile("dataset/IOT-temp.csv");


        /*DataSet<Tuple4<String, String, Double, String>> dataset = env.readCsvFile("dataset/IOT-temp.csv")
            .ignoreFirstLine()
            .ignoreInvalidLines()
            .includeFields(true, false, true, true, true)
            .types(String.class, String.class, Double.class, String.class);*/

        SingleOutputStreamOperator<SensorRecord> records = text.map(new MapFunction<String, SensorRecord>() {
            @Override
            public SensorRecord map(String item) throws Exception {
                String[] items = item.split(",");
                //Make sure the first line on data is removed, as streaming can't ignore it
                String id = items[0];
                LocalDateTime eventDate = LocalDateTime.parse(items[2], DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"));
                Double temperature = Double.parseDouble(items[3]);
                SensorInOut inOut = items[4].equals("In") ? SensorInOut.IN : SensorInOut.OUT;
                return new SensorRecord(id, eventDate, temperature, inOut);
            }
        });

        SingleOutputStreamOperator<SensorRecord> recordsIn = records.filter(new FilterFunction<SensorRecord>() {
            @Override
            public boolean filter(SensorRecord record) throws Exception {
                return record.getInOut().equals(SensorInOut.IN);
            }
        });

        SingleOutputStreamOperator<SensorRecord> recordsOut = records.filter(new FilterFunction<SensorRecord>() {
            @Override
            public boolean filter(SensorRecord record) throws Exception {
                return record.getInOut().equals(SensorInOut.OUT);
            }
        });

        //recordsIn.print();
        System.out.print(records.keyBy(SensorRecord::getEventDate).window(TumblingEventTimeWindows.of(Time.milliseconds(60))));

        //text = text.sortPartition(2, Order.DESCENDING);
        //text.print();

        //Date format?
        //Date date1=new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
        // execute program
        env.execute("Flink Batch Java API Skeleton");
    }
}
