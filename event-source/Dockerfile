# Build Stage
FROM maven:3.6.0-jdk-11-slim AS builder
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean package

# Package Stage
FROM openjdk:8-jdk-alpine
ARG JAR_FILE=/home/app/target/*.jar
COPY --from=builder ${JAR_FILE} /home/app/target/app.jar
ENTRYPOINT ["java","-jar","/app.jar"]

## One-stage build up
#FROM openjdk:8-jdk-alpine
#ARG JAR_FILE=target/*.jar
#COPY ${JAR_FILE} app.jar
#ENTRYPOINT ["java","-jar","/app.jar"]
