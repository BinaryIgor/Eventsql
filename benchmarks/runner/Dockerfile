FROM eclipse-temurin:21-alpine

COPY target/eventsql-benchmarks-runner-jar-with-dependencies.jar /app.jar

ENTRYPOINT ["java", "-jar", "/app.jar"]