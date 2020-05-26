# Stream processing of Sensor Temperature Data in Real-Time using Kafka and Flink to calculate relation between rooms and alert based on rules
## Big Data Management Course 2020

Contributors:
- Juan Carlos Ramos
- Karl-Walter Sillaots

In this project our goal was to develop a streaming system which can do real-time analysis of temperature data of multiple room sensors. Two types of analysis were done: compute the relation between temperatures from two different sources, and trigger alerts based on real time conditions. Large number of sensors should be able to send data in real-time, and should be able to receive the feedback and result in a comprehensible time in a dashboard. Keeping the requirements in mind we used Apache Kafka and Flink to develop the system. In this setting, a custom application runs Kafka producer client, which is sending data in real-time with randomed sensor data. Flink streaming is used to process data coming from Kafka on a single topic to analyze the temperature measurements. We have developed the system as a visual and interactive dashboard tool.


# How to run

## Setup the environment
The project has been packed to easily work with docker containers, that's why the only requirement to have is Docker.
Once we have Docker up and running:

- Clone the repository
```shell script
git clone https://github.com/0x7b1/heatmap-alert-app.git
```


## Start the Kafka cluster

- Navigate to the folder `event-source`
```shell script
cd event-source
```
- Build and run the Kafka cluster on background
```shell script
docker-compose up -p kafka-cluster -d --build
```

## Start the Flink cluster

- Go back again to the root folder
```shell script
cd ..
```
- Build and run the Flink cluster on background
```shell script
docker-compose up -p flink-cluster -d --build
```

It might take some time to have both clusters built and running, but once we have it we can access to the following set of available ports.
- **8080**: Grafana dashboard
- **8081**: Flink Web dashboard
- **9000**: Kafka Manager
- **9090**: Prometheus Dashboard



# System Design

The system is designed using Kafka and Flink.
Kafka is used to produce and receive temperature sensor measurements.
Flink is used to receive the temperature records from a single Kafka topic.
Flink is also used to calculate the relation between different temperature sources over the windows of data streams, as well as generating alerts based on certain threshold conditions.

## Apache Kafka

Apache Kafka is a distributed, high-throughput message queuing system designed for making streaming data available to multiple data consumers.
Kafka makes the streaming data persistent by storing incoming messages using a log data structure.
This allows multiple stream consumers to read the stream at different positions (offsets) and different speeds, and also read messages from a certain point of time.
The most important abstraction in kafka is the topic. A topic is a category name to which records are stored and published. Topics are divided into partitions. Partitions allow topics to be parallelized by splitting the data into a particular topic across different physical nodes.
Services that put data into a topic are producers, and consumers are those read from a topic.
In our current system abstraction, sensor data is published to a particular topic designated for the stream.
This record corresponds to a particular room identifier within a defined incoming source, meaning “out” or “in”.
This topic matches with a set of partitions that is meant to be used by each particular room.
This capability is used to guarantee the ordeness or message processing.

## Apache Flink

Apache Flink is a scalable and fault tolerant data stream processing framework that runs on self-contained streaming computations that can be deployed standalone or using a resource manager. Flink consumes streams and produces data into streams.
Flink is a true streaming engine, treating batch as a special case with bounded data.
This feature is quite the opposite compared to other streaming frameworks like Spark Streaming or Kafka Streaming.
Flink is commonly used with Kafka as the underlying storage layer, but is independent of it.
Flink’s rich API allows to model a problem using data transformations following the paradigm of data flow. It also lets us use advanced features such as watermarks and timestamps, and Complex Event Processing (CEP). 
For our system, we make intensive use of data transformation functions, event windowing and also pattern matching.

# System Architecture

![](docs/architecture.png)

The system architecture is shown above. It all starts by a single Kafka program which acts as a data producer in the Kafka model.
The Kafka instance runs along with Zookeeper, as the service for coordination and configuration within the cluster.
The Kafka manager helps by providing a web interface to see and manage the current state of Kafka and Zookeeper configurations.
The produced data are temperature readings, simulating to those who come from real sensors. This data is published into a specific topic for further processing.
In this system the assumption is that any sensor can join or leave the streaming pipeline and be able to persist data at any point in time.
Kafka takes care of the scalability of the system as the number of incoming temperature records grow or shrink.
It also serves as a data retention, storage and forwarding interface.
Ideally kafka treats every message from a specific room into a corresponding partition, but it may be also the case that there are less partitions than topics. For such a case a round robin technique is used.

When data arrives to Flink, two tasks are going to process them in the data flow pipeline, “Temperature Relation Processing”, and “Temperature Alerting” which are going to be detailed next.
Later, the results are sinked into InfluxDB as a time-series event. 
This output can also be sent back to Kafka for further processing but we just want to deliver the results as early as we have.
Prometheus will constantly monitor Flink to retrieve and store system metrics used later for the evaluation.
On the last frontier we have Grafana that pulls data from Prometheus and InfluxDB every interval of time to show in an interactive dashboard the results of the data pipeline scheme.

## Data Pipeline

The algorithms used in the data transformation along the flow of the graph are discussed in more detail in the next section.
When new data arrives to Flink from Kafka, the consumer will split into two main branches making reference to the incoming source, either from “IN” or “OUT”.
Each task will be processed by a single Flink Task Manager. Also we adjust the level of parallelism to match the Kafka partitions into Flink Slots, in that way we guarantee correct order of arrival of messages.

The figure below shows the complete data pipeline including the processing of Temperature Alerts. At first glance it seems that the splitting of data is further processing and filtered out. At the end data joins again to output to a single sink in each case.
For the temperature relation part, data is sent to two sinks, each corresponding to a specific source. An extra sink is used to output to the computed relation.
The alerting does not sink to any particular source, but it keeps an internal counter meant to be fetched by Prometheus.

![](docs/dataflow2.png)

# Experiments and results

The prototype system was implemented in Java.
The program that simulates the sensor temperatures is a Java application that resides in the same cluster as Kafka.
The implementation of the two temperature tasks are Java applications which are sent to a separate cluster for Flink.
The clusters do both real-time analysis and produce the results to be shown in a custom made Grafana dashboard.


![](docs/d_all.png)

The first experiment was made with 5 room sensors under a load of emission of 5 random temperatures per second.
The figure shows: 
- The results in two plots: A heatmap that visually shows the relation between the temperatures IN and OUT, being the darker one considered as the OUT greater than IN.
- The set of gauges per room, which show three different colors based on three thresholds (10, 20, 30)
- When two consecutive warnings are emitted then new alerts are shown.
- The graphs related to the performance evaluation. We are mainly plotting the memory usage of the Flink Job Manager and Task Manager. With this we want to know the task needs memory in order to get the results. We noticed that Flink does a great balance among different Task Managers when more parallelism is enabled.
- Plot of the average latency for the Flink pipeline in each pass under the defined load. We see that it does not fluctuate violently, keeping in the range of 300 - 350 ms.

Further improvement can be done from this point. The CEP patterns can be fine tuned to detect trends in temperature. Also we could potentially forecast new scenarios based on predefined window times. Also, it would come in handy to have a better random producer of data measurements, for example by taking a real data set and feeding it into the current streaming system. 


For further reference consult to the project report.
