# syntax=docker/dockerfile:1.7

############################################
# Build stage
############################################
FROM maven:3.9.9-eclipse-temurin-21-alpine AS build

WORKDIR /workspace

# Copy root and aggregator POMs first (better cache for dependency download)
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY alfresco/pom.xml alfresco/pom.xml
COPY nuxeo/pom.xml nuxeo/pom.xml

# Copy all module POMs (all needed for Maven reactor resolution)
COPY common/content-lake-repo-model/pom.xml common/content-lake-repo-model/pom.xml
COPY common/content-lake-spi/pom.xml common/content-lake-spi/pom.xml
COPY common/content-lake-core/pom.xml common/content-lake-core/pom.xml
COPY common/rag-service/pom.xml common/rag-service/pom.xml
COPY alfresco/content-lake-source-alfresco/pom.xml alfresco/content-lake-source-alfresco/pom.xml
COPY alfresco/alfresco-batch-ingester/pom.xml alfresco/alfresco-batch-ingester/pom.xml
COPY alfresco/alfresco-live-ingester/pom.xml alfresco/alfresco-live-ingester/pom.xml
COPY nuxeo/content-lake-source-nuxeo/pom.xml nuxeo/content-lake-source-nuxeo/pom.xml
COPY nuxeo/nuxeo-batch-ingester/pom.xml nuxeo/nuxeo-batch-ingester/pom.xml
COPY nuxeo/nuxeo-live-ingester/pom.xml nuxeo/nuxeo-live-ingester/pom.xml

# Pre-fetch dependencies (cached with BuildKit)
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -DskipTests -pl alfresco/alfresco-live-ingester -am dependency:go-offline

# Copy only the sources needed for live-ingester and its transitive dependencies
COPY common/content-lake-spi/src common/content-lake-spi/src
COPY common/content-lake-core/src common/content-lake-core/src
COPY alfresco/content-lake-source-alfresco/src alfresco/content-lake-source-alfresco/src
COPY alfresco/alfresco-live-ingester/src alfresco/alfresco-live-ingester/src

# Build
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -DskipTests -pl alfresco/alfresco-live-ingester -am package

# Extract Spring Boot layers (smaller rebuilds when code changes)
RUN mkdir -p /layers \
 && JAR="$(ls -1 alfresco/alfresco-live-ingester/target/*.jar | grep -v 'original' | head -n 1)" \
 && java -Djarmode=layertools -jar "$JAR" extract --destination /layers


############################################
# Runtime stage
############################################
FROM eclipse-temurin:21-jre-alpine

RUN addgroup -S app && adduser -S -G app app

WORKDIR /app

COPY --from=build /layers/dependencies/ ./
COPY --from=build /layers/snapshot-dependencies/ ./
COPY --from=build /layers/spring-boot-loader/ ./
COPY --from=build /layers/application/ ./

USER app

EXPOSE 9092

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD wget -qO- http://localhost:9092/actuator/health || exit 1

ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+UseContainerSupport -Dfile.encoding=UTF-8"

ENTRYPOINT ["java","org.springframework.boot.loader.launch.JarLauncher"]
