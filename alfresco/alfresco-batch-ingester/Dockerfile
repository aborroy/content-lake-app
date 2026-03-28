# syntax=docker/dockerfile:1.7

############################################
# Build stage
############################################
FROM maven:3.9.9-eclipse-temurin-21-alpine AS build

WORKDIR /workspace

# Copy only POMs first (better cache for dependency download)
# All module poms are required so Maven can resolve the full reactor
COPY pom.xml .
COPY content-lake-common/pom.xml content-lake-common/pom.xml
COPY content-lake-repo-model/pom.xml content-lake-repo-model/pom.xml
COPY batch-ingester/pom.xml batch-ingester/pom.xml
COPY live-ingester/pom.xml live-ingester/pom.xml
COPY rag-service/pom.xml rag-service/pom.xml

# Pre-fetch dependencies (cached with BuildKit)
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -DskipTests -pl batch-ingester -am dependency:go-offline

# Copy sources and build
COPY content-lake-common/src content-lake-common/src
COPY batch-ingester/src batch-ingester/src

RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -DskipTests -pl batch-ingester -am package

# Extract Spring Boot layers (smaller rebuilds when code changes)
RUN mkdir -p /layers \
 && JAR="$(ls -1 batch-ingester/target/*.jar | grep -v 'original' | head -n 1)" \
 && java -Djarmode=layertools -jar "$JAR" extract --destination /layers


############################################
# Runtime stage
############################################
FROM eclipse-temurin:21-jre-alpine

# Non-root user
RUN addgroup -S app && adduser -S -G app app

WORKDIR /app

# Copy extracted layers
COPY --from=build /layers/dependencies/ ./
COPY --from=build /layers/snapshot-dependencies/ ./
COPY --from=build /layers/spring-boot-loader/ ./
COPY --from=build /layers/application/ ./

USER app

EXPOSE 9090

ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+UseContainerSupport -Dfile.encoding=UTF-8"

ENTRYPOINT ["java","org.springframework.boot.loader.launch.JarLauncher"]
