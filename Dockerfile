FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY src src
COPY contracts contracts
RUN mvn -B -ntp -DskipTests package
FROM gcr.io/distroless/java21-debian13:nonroot
WORKDIR /app
COPY --from=build /build/target/ingestion-runtime-*.jar app.jar
USER 10002:10002
STOPSIGNAL SIGTERM
ENTRYPOINT ["java","-jar","/app/app.jar"]
