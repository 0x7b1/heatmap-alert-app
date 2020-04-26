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

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


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
 * 		./bin/flink run -c ut.bigdata.StreamingJob target/heatmap-alert-app-1.0-SNAPSHOT.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class BigDataBatch {

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<String, String, String, String, String>> text = env.readCsvFile(args[0]).types(String.class, String.class, String.class, String.class, String.class);
                                    // Takes the path given as a program argument and reads it.
                                    // Apparently this is an example to read a... csv file for streaming, if I have to do that?
                                    // https://github.com/ververica/flink-training-exercises/blob/701d69d5d5007a0db2fa70b4f500357c4e74c110/src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/windows/DrivingSessions.java#L59
                                    // Good example for using Batch
                                    // https://ci.apache.org/projects/flink/flink-docs-stable/dev/batch/examples.html

        //text = text.sortPartition(2, Order.DESCENDING);
        text.print();
        System.out.println(text.count());

        //Date format?
        //Date date1=new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
        // execute program
        //env.execute("Flink Batch Java API Skeleton");
    }
}
