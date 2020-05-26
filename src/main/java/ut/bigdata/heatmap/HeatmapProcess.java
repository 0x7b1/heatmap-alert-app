package ut.bigdata.heatmap;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import ut.bigdata.heatmap.models.TemperatureAlert;
import ut.bigdata.heatmap.models.TemperatureRecord;
import ut.bigdata.heatmap.models.TemperatureWarning;
import ut.bigdata.heatmap.transformations.*;

import java.util.*;

public class HeatmapProcess {
    static String inputTopic = "sensor_temperatures";
    static String consumerGroup = "sensor_consumer";

        static String kafkaAddress = "kafka:9092";
//    static String kafkaAddress = "localhost:29092";

//    static String influxDBHost = "http://localhost:8086";
    static String influxDBHost = "http://influxdb:8086";

    static String influxDBName = "sensor_temperatures";
    static String influxDBUser = "admin";
    static String influxDBPassword = "admin";
    static String influxMeasurementAvg = "rooms_avg";
    static String influxMeasurementTemperatures = "rooms_temperatures";

    static int THRESHOLD_TEMPERATURE_ALERT = 30;

    public static void processTemperatureRelations(KeyedStream<TemperatureRecord, String> inKeyed, KeyedStream<TemperatureRecord, String> outKeyed) {
        DataStream<Tuple3<String, Double, Long>> in = inKeyed
            .timeWindow(Time.seconds(5))
            .apply(new TemperatureAverager());

        DataStream<Tuple3<String, Double, Long>> out = outKeyed
            .timeWindow(Time.seconds(5))
            .apply(new TemperatureAverager());

        // Finding the relation between temperatures IN/OUT
        DataStream<Tuple3<String, Double, Long>> temperatureRoomRelations = in
            .join(out)
            .where(e -> e.f0)
            .equalTo(e -> e.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(1)))
            .apply(new JoinWindowsToTemperatureRelation());

        // Sinking to InfluxDB
        InfluxDBSink influxDBSink = DataBuilder.createInfluxSink(influxDBHost, influxDBName, influxDBUser, influxDBPassword);
        temperatureRoomRelations
            .map(new TemperatureAvgRoomsToInfluxDataPoint(influxMeasurementAvg))
            .addSink(influxDBSink);

        // Sink to stdout
        temperatureRoomRelations.print();

        // Sinking IN/OUT room temperatures
        in.map(new TemperatureRoomsToInfluxDataPoint(influxMeasurementTemperatures, "IN")).addSink(influxDBSink);
        out.map(new TemperatureRoomsToInfluxDataPoint(influxMeasurementTemperatures, "OUT")).addSink(influxDBSink);

    }

    public static void processAlerting(KeyedStream<TemperatureRecord, String> inKeyed, KeyedStream<TemperatureRecord, String> outKeyed) {
        // Create a pattern over averaged temperatures in windows that are going above the threshold within an interval
        Pattern<TemperatureRecord, ?> warningPattern = Pattern
            .<TemperatureRecord>begin("first")
            .subtype(TypeInformation.of(new TypeHint<TemperatureRecord>() {
            }).getTypeClass())
            .where(new IterativeCondition<TemperatureRecord>() {
                @Override
                public boolean filter(TemperatureRecord record, Context<TemperatureRecord> context) throws Exception {
                    return record.getTemperature() >= THRESHOLD_TEMPERATURE_ALERT;
                }
            })
            .next("second")
            .subtype(TypeInformation.of(new TypeHint<TemperatureRecord>() {
            }).getTypeClass())
            .where(new IterativeCondition<TemperatureRecord>() {
                @Override
                public boolean filter(TemperatureRecord record, Context<TemperatureRecord> context) throws Exception {
                    return record.getTemperature() >= THRESHOLD_TEMPERATURE_ALERT;
                }
            })
            .within(Time.seconds(5));

        // Create an alert if two consecutive warnings appear in the given interval
        Pattern<TemperatureWarning, ?> alertPattern = Pattern
            .<TemperatureWarning>begin("first")
            .next("second")
            .within(Time.seconds(10));

        // Create the pattern stream for warning pattern (IN)
        PatternStream<TemperatureRecord> warningPatternStreamIn = CEP.pattern(
            inKeyed,
            warningPattern
        );

        // Create temperature warning stream for each warning matched pattern (IN)
        DataStream<TemperatureWarning> warningsIn = warningPatternStreamIn.select(
            new PatternSelectFunction<TemperatureRecord, TemperatureWarning>() {
                @Override
                public TemperatureWarning select(Map<String, List<TemperatureRecord>> pattern) throws Exception {
                    TemperatureRecord first = (TemperatureRecord) pattern.get("first").get(0);
                    TemperatureRecord second = (TemperatureRecord) pattern.get("second").get(0);

                    double avgTemperatures = (first.getTemperature() + second.getTemperature()) / 2;
                    return new TemperatureWarning(first.getRoomId(), avgTemperatures);
                }
            }
        );

        // Create the pattern stream for alert pattern (IN)
        PatternStream<TemperatureWarning> alertPatternStreamIn = CEP.pattern(
            warningsIn,
            alertPattern
        );

        // Create the temperature alert only if the second temperature warning avg is higher than the first one (IN)
        DataStream<TemperatureAlert> alertsIn = alertPatternStreamIn.flatSelect(
            new PatternFlatSelectFunction<TemperatureWarning, TemperatureAlert>() {
                @Override
                public void flatSelect(
                    Map<String, List<TemperatureWarning>> pattern,
                    Collector<TemperatureAlert> collector) throws Exception {
                    TemperatureWarning first = pattern.get("first").get(0);
                    TemperatureWarning second = pattern.get("second").get(0);

                    if (first.getAvgTemperature() < second.getAvgTemperature()) {
                        collector.collect(new TemperatureAlert(first.getRoomId(), second.getAvgTemperature()));
                    }
                }
            })
            .map(new RichMapFunction<TemperatureAlert, TemperatureAlert>() {
                private transient Counter eventCounter;

                @Override
                public void open(Configuration parameters) throws Exception {
                    eventCounter = getRuntimeContext().getMetricGroup().counter("veneco");
                    super.open(parameters);
                }

                @Override
                public TemperatureAlert map(TemperatureAlert temperatureAlertPattern) throws Exception {
                    eventCounter.inc();
                    return temperatureAlertPattern;
                }
            });

        // Create the pattern stream for warning pattern (OUT)
        PatternStream<TemperatureRecord> warningPatternStreamOut = CEP.pattern(
            outKeyed,
            warningPattern
        );


        // Create temperature warning stream for each warning matched pattern (OUT)
        DataStream<TemperatureWarning> warningsOut = warningPatternStreamIn.select(
            new PatternSelectFunction<TemperatureRecord, TemperatureWarning>() {
                @Override
                public TemperatureWarning select(Map<String, List<TemperatureRecord>> pattern) throws Exception {
                    TemperatureRecord first = (TemperatureRecord) pattern.get("first").get(0);
                    TemperatureRecord second = (TemperatureRecord) pattern.get("second").get(0);

                    double avgTemperatures = (first.getTemperature() + second.getTemperature()) / 2;
                    return new TemperatureWarning(first.getRoomId(), avgTemperatures);
                }
            }
        );

        // Create the pattern stream for alert pattern (OUT)
        PatternStream<TemperatureWarning> alertPatternStreamOut = CEP.pattern(
            warningsOut,
            alertPattern
        );

        // Create the temperature alert only if the second temperature warning avg is higher than the first one (OUT)
        DataStream<TemperatureAlert> alertsOut = alertPatternStreamOut.flatSelect(
            new PatternFlatSelectFunction<TemperatureWarning, TemperatureAlert>() {
                @Override
                public void flatSelect(
                    Map<String, List<TemperatureWarning>> pattern,
                    Collector<TemperatureAlert> collector) throws Exception {
                    TemperatureWarning first = pattern.get("first").get(0);
                    TemperatureWarning second = pattern.get("second").get(0);

                    if (first.getAvgTemperature() < second.getAvgTemperature()) {
                        collector.collect(new TemperatureAlert(first.getRoomId(), second.getAvgTemperature()));
                    }
                }
            })
            .map(new RichMapFunction<TemperatureAlert, TemperatureAlert>() {
                private transient Counter eventCounter;

                @Override
                public void open(Configuration parameters) throws Exception {
                    eventCounter = getRuntimeContext().getMetricGroup().counter("veneco");
                    super.open(parameters);
                }

                @Override
                public TemperatureAlert map(TemperatureAlert temperatureAlertPattern) throws Exception {
                    eventCounter.inc();
                    return temperatureAlertPattern;
                }
            });
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer<TemperatureRecord> kafkaConsumerSource =
            DataBuilder.createTemperatureConsumer(inputTopic, kafkaAddress, consumerGroup);

        // Reading input stream from kafka topic
        DataStream<TemperatureRecord> temperatureRecords = env
            .addSource(kafkaConsumerSource)
            .assignTimestampsAndWatermarks(new TemperatureRecordTimestampExtractor(Time.seconds(3)))
            // This will spread messages from partitions evenly across flink workers
            .rebalance();

        // Input stream of IN sensor events
        KeyedStream<TemperatureRecord, String> inKeyed = temperatureRecords
            .filter(e -> e.getSource().equals("IN"))
            .keyBy(e -> e.getRoomId());

        // Input stream of OUT sensor events
        KeyedStream<TemperatureRecord, String> outKeyed = temperatureRecords
            .filter(e -> e.getSource().equals("OUT"))
            .keyBy(e -> e.getRoomId());

        processTemperatureRelations(inKeyed, outKeyed);
        processAlerting(inKeyed, outKeyed);

        System.out.println(env.getExecutionPlan());

        env.execute("Relation between temperatures IN/OUT per room");
    }
}
