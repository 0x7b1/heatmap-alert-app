## Build Stage
#FROM maven:3.6.0-jdk-11-slim AS builder
#COPY src /home/app/src
#COPY pom.xml /home/app
#RUN mvn -f /home/app/pom.xml clean package

## Package Stage
#FROM flink:1.10.0
#COPY config/flink $FLINK_HOME/conf
#RUN mv $FLINK_HOME/opt/flink-metrics-prometheus-*.jar $FLINK_HOME/lib
#COPY --from=builder /home/app/target/*.jar $FLINK_HOME/lib/

# One-stage build up
FROM openjdk:8-jdk-alpine
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
