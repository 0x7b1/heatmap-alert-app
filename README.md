# Heatmap alert App

This project is a streaming implementation of sensor temperatures. It is heavily based on https://www.kaggle.com/atulanandjha/temperature-readings-iot-devices

## How to run

This project depends upon two clusters.
The first contains the dependencies for Kafka, Zookeeper, and the consumer/producer for temperature data 

```shell script
$ git clone https://github.com/0x7b1/heatmap-alert-app.git
```


```shell script
$ cd event-source
$ docker-compose up -d --build
```

The second cluster contains the orchestration for Flink, Prometheus, InfluxDB, and Grafana

```shell script
$ cd .. # go to root again
$ docker-compose up -d --build
```

## TODO

- test .rebalance()
