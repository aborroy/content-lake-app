# syntax=docker/dockerfile:1.7

FROM maven:3.9.9-eclipse-temurin-21-alpine AS build

WORKDIR /workspace

COPY pom.xml .
COPY content-lake-spi/pom.xml content-lake-spi/pom.xml
COPY content-lake-core/pom.xml content-lake-core/pom.xml
COPY content-lake-source-alfresco/pom.xml content-lake-source-alfresco/pom.xml
COPY content-lake-source-nuxeo/pom.xml content-lake-source-nuxeo/pom.xml
COPY content-lake-repo-model/pom.xml content-lake-repo-model/pom.xml
COPY alfresco-batch-ingester/pom.xml alfresco-batch-ingester/pom.xml
COPY nuxeo-batch-ingester/pom.xml nuxeo-batch-ingester/pom.xml
COPY alfresco-live-ingester/pom.xml alfresco-live-ingester/pom.xml
COPY nuxeo-live-ingester/pom.xml nuxeo-live-ingester/pom.xml
COPY rag-service/pom.xml rag-service/pom.xml

RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -DskipTests -pl nuxeo-batch-ingester -am dependency:go-offline

COPY content-lake-spi/src content-lake-spi/src
COPY content-lake-core/src content-lake-core/src
COPY content-lake-source-nuxeo/src content-lake-source-nuxeo/src
COPY nuxeo-batch-ingester/src nuxeo-batch-ingester/src
COPY nuxeo-live-ingester/src nuxeo-live-ingester/src

RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -DskipTests -pl nuxeo-batch-ingester -am package

RUN mkdir -p /layers \
 && JAR="$(ls -1 nuxeo-batch-ingester/target/*.jar | grep -v 'original' | head -n 1)" \
 && java -Djarmode=layertools -jar "$JAR" extract --destination /layers

FROM eclipse-temurin:21-jre-alpine

RUN addgroup -S app && adduser -S -G app app

WORKDIR /app

COPY --from=build /layers/dependencies/ ./
COPY --from=build /layers/snapshot-dependencies/ ./
COPY --from=build /layers/spring-boot-loader/ ./
COPY --from=build /layers/application/ ./

USER app

EXPOSE 9093

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD wget -qO- http://localhost:9093/actuator/health || exit 1

ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+UseContainerSupport -Dfile.encoding=UTF-8"

ENTRYPOINT ["java","org.springframework.boot.loader.launch.JarLauncher"]
