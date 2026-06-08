# syntax=docker/dockerfile:1
#
# Multi-stage build: compile the shaded job jar with Maven (JDK 11), then layer
# it onto the official Flink 1.18 / Java 11 runtime with the Prometheus metrics
# reporter and S3 filesystem plugins enabled.

FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q dependency:go-offline || true
COPY src ./src
RUN mvn -B -q clean package -DskipTests

FROM flink:1.18-java11
ENV FLINK_HOME=/opt/flink

# The Prometheus reporter ships as a bundled plugin already
# (/opt/flink/plugins/metrics-prometheus). Only the S3 (presto) filesystem
# needs to be promoted from opt/ into plugins/ to be loadable.
RUN mkdir -p $FLINK_HOME/plugins/s3-fs-presto && \
    cp $FLINK_HOME/opt/flink-s3-fs-presto-*.jar $FLINK_HOME/plugins/s3-fs-presto/

# Application-mode job jar location.
COPY --from=build /build/target/flink-stress-harness.jar $FLINK_HOME/usrlib/flink-stress-harness.jar
