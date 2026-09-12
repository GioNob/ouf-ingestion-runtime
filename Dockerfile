FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY src src
RUN mvn -B -ntp -DskipTests package
FROM eclipse-temurin:21-jre
RUN groupadd -g 10002 ouf && useradd -r -u 10002 -g ouf ouf
WORKDIR /app
COPY --from=build /build/target/ingestion-runtime-*.jar app.jar
USER 10002:10002
ENTRYPOINT ["java","-jar","/app/app.jar"]
