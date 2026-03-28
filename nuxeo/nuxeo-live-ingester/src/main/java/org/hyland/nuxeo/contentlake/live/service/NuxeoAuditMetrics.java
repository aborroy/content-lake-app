package org.hyland.nuxeo.contentlake.live.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
public class NuxeoAuditMetrics {

    private final MeterRegistry meterRegistry;
    private final Clock clock;

    private final ConcurrentMap<String, AtomicLong> cursorLagMs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicLong> cursorEpochMs = new ConcurrentHashMap<>();

    public void recordEvent(String repositoryKey, String eventId, String outcome) {
        Counter.builder("contentlake.nuxeo.audit.events.total")
                .tag("repository", repositoryKey)
                .tag("eventId", eventId)
                .tag("outcome", outcome)
                .register(meterRegistry)
                .increment();
    }

    public Timer.Sample startPoll() {
        return Timer.start(meterRegistry);
    }

    public void recordPoll(String repositoryKey, String outcome, Timer.Sample sample) {
        sample.stop(Timer.builder("contentlake.nuxeo.audit.poll.duration")
                .tag("repository", repositoryKey)
                .tag("outcome", outcome)
                .register(meterRegistry));
        Counter.builder("contentlake.nuxeo.audit.polls.total")
                .tag("repository", repositoryKey)
                .tag("outcome", outcome)
                .register(meterRegistry)
                .increment();
    }

    public void updateCursor(String repositoryKey, OffsetDateTime logDate) {
        long epochMillis = logDate.toInstant().toEpochMilli();
        cursorEpochGauge(repositoryKey).set(epochMillis);
        long lag = Math.max(0L, OffsetDateTime.now(clock).toInstant().toEpochMilli() - epochMillis);
        cursorLagGauge(repositoryKey).set(lag);
    }

    private AtomicLong cursorLagGauge(String repositoryKey) {
        return cursorLagMs.computeIfAbsent(repositoryKey, key -> {
            AtomicLong gaugeValue = new AtomicLong();
            Gauge.builder("contentlake.nuxeo.audit.cursor.lag.ms", gaugeValue, AtomicLong::get)
                    .tag("repository", repositoryKey)
                    .register(meterRegistry);
            return gaugeValue;
        });
    }

    private AtomicLong cursorEpochGauge(String repositoryKey) {
        return cursorEpochMs.computeIfAbsent(repositoryKey, key -> {
            AtomicLong gaugeValue = new AtomicLong();
            Gauge.builder("contentlake.nuxeo.audit.cursor.timestamp.ms", gaugeValue, AtomicLong::get)
                    .tag("repository", repositoryKey)
                    .baseUnit("milliseconds")
                    .register(meterRegistry);
            return gaugeValue;
        });
    }
}
