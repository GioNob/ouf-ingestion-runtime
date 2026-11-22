FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY src src
COPY contracts contracts
COPY vendor/authorization-sdk vendor/authorization-sdk
RUN cd vendor/authorization-sdk && sha256sum -c SOURCE_SHA256SUMS && timeout --signal=TERM --kill-after=30s 10m mvn -B -ntp clean install
RUN mvn -B -ntp -DskipTests package
FROM eclipse-temurin:21-jre-alpine
RUN apk upgrade --no-cache \
    && addgroup -g 10002 ouf \
    && adduser -S -D -H -u 10002 -G ouf ouf
WORKDIR /app
COPY --from=build /build/target/ingestion-runtime-*.jar app.jar
USER 10002:10002
STOPSIGNAL SIGTERM
ENTRYPOINT ["java","-jar","/app/app.jar"]
