#FROM gradle:6.3 as builder
#COPY . .
#RUN gradle shadowJar

#FROM flink:1.10.0
#RUN echo "metrics.reporters: prom" >> "$FLINK_HOME/conf/flink-conf.yaml"; \
#    echo "metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter" >> "$FLINK_HOME/conf/flink-conf.yaml"; \
#    mv $FLINK_HOME/opt/flink-metrics-prometheus-*.jar $FLINK_HOME/lib
#COPY --from=builder /home/gradle/build/libs/*.jar $FLINK_HOME/lib/

###########

## Start with a base image containing Java runtime
#FROM openjdk:8-jdk-alpine
#
## Add a volume pointing to /tmp
#VOLUME /tmp
#
## Make port 5656 available to the world outside this container
#EXPOSE 5656
#
#ADD target/heatmap-alert-app-*.jar heatmap-alert-app.jar
#
## Run the jar file
#ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-Dspring.profiles.active=dev","-jar","/heatmap-alert.jar"]
#
#FROM flink:1.10.0
#RUN echo "metrics.reporters: prom" >> "$FLINK_HOME/conf/flink-conf.yaml"; \
#    echo "metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter" >> "$FLINK_HOME/conf/flink-conf.yaml"; \
#    mv $FLINK_HOME/opt/flink-metrics-prometheus-*.jar $FLINK_HOME/lib
#COPY --from=builder /home/gradle/build/libs/*.jar $FLINK_HOME/lib/


#FROM openjdk:8-jdk-alpine
#
## Install Bash
#RUN apk add --no-cache bash libc6-compat
#
## Copy resources
#WORKDIR /
#COPY wait-for-it.sh wait-for-it.sh
#COPY target/kafka-spark-flink-example-1.0-SNAPSHOT-jar-with-dependencies.jar kafka-spark-flink-example.jar
#
## Wait for Zookeeper and Kafka to be available and run application
#CMD ./wait-for-it.sh -s -t 30 $EXAMPLE_ZOOKEEPER_SERVER -- ./wait-for-it.sh -s -t 30 $EXAMPLE_KAFKA_SERVER -- java -Xmx512m -jar kafka-spark-flink-example.jar

# Build Stage
FROM maven:3.6.0-jdk-11-slim AS builder
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean package

# Package Stage
FROM flink:1.10.0
COPY --from=builder /home/app/target/heatmap-alert-app-*.jar $FLINK_HOME/lib/

