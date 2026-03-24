package org.alfresco.contentlake.batch.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
@RequiredArgsConstructor
public class IngestionJob {

    public enum JobStatus { RUNNING, COMPLETED, FAILED }

    private final String jobId;

    private volatile JobStatus status = JobStatus.RUNNING;

    private final Instant startedAt = Instant.now();
    private volatile Instant completedAt;

    @JsonIgnore
    private final AtomicInteger discoveredCount = new AtomicInteger(0);

    @JsonIgnore
    private final AtomicInteger metadataIngestedCount = new AtomicInteger(0);

    @JsonIgnore
    private final AtomicInteger failedCount = new AtomicInteger(0);

    public void incrementDiscovered() {
        discoveredCount.incrementAndGet();
    }

    public void incrementMetadataIngested() {
        metadataIngestedCount.incrementAndGet();
    }

    public void incrementFailed() {
        failedCount.incrementAndGet();
    }

    public void complete() {
        status = JobStatus.COMPLETED;
        completedAt = Instant.now();
    }

    public void fail() {
        status = JobStatus.FAILED;
        completedAt = Instant.now();
    }

    // ---- JSON-facing getters (plain ints) ----

    @JsonProperty("discoveredCount")
    public int getDiscoveredCountValue() {
        return discoveredCount.get();
    }

    @JsonProperty("metadataIngestedCount")
    public int getMetadataIngestedCountValue() {
        return metadataIngestedCount.get();
    }

    @JsonProperty("failedCount")
    public int getFailedCountValue() {
        return failedCount.get();
    }
}