FROM eclipse-temurin:21-jre-alpine

RUN addgroup -S app && adduser -S app -G app
WORKDIR /app

COPY target/live-ingester-1.0.0-SNAPSHOT.jar app.jar

RUN chown -R app:app /app
USER app

EXPOSE 9092

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD wget -qO- http://localhost:9092/actuator/health || exit 1

ENTRYPOINT ["java", "-jar", "app.jar"]
