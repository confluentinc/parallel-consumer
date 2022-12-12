FROM amazoncorretto:17-alpine3.16
COPY ./parallel-consumer-examples/parallel-consumer-example-core/target/pc-core-metrics-jar-with-dependencies.jar /tmp
ENTRYPOINT ["java", "-jar", "/tmp/pc-core-metrics-jar-with-dependencies.jar"]
