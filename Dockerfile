# Build Stage
FROM maven:3.6.0-jdk-11-slim AS builder
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean package

# Package Stage
FROM flink:1.10.0
RUN echo "metrics.reporters: prom" >> "$FLINK_HOME/conf/flink-conf.yaml"; \
    echo "metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter" >> "$FLINK_HOME/conf/flink-conf.yaml"; \
    mv $FLINK_HOME/opt/flink-metrics-prometheus-*.jar $FLINK_HOME/lib
COPY --from=builder /home/app/target/*.jar $FLINK_HOME/lib/

## One-stage build up
#FROM flink:1.10.0
#RUN echo "metrics.reporters: prom" >> "$FLINK_HOME/conf/flink-conf.yaml"; \
#    echo "metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter" >> "$FLINK_HOME/conf/flink-conf.yaml"; \
#    mv $FLINK_HOME/opt/flink-metrics-prometheus-*.jar $FLINK_HOME/lib
#COPY target/*.jar $FLINK_HOME/lib/
