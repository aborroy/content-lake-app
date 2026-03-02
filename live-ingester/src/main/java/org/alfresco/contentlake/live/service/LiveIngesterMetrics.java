package org.alfresco.contentlake.live.service;

import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

@Service
public class LiveIngesterMetrics {

    private final AtomicLong receivedCount = new AtomicLong();
    private final AtomicLong processedCount = new AtomicLong();
    private final AtomicLong filteredCount = new AtomicLong();
    private final AtomicLong deduplicatedCount = new AtomicLong();
    private final AtomicLong errorCount = new AtomicLong();

    public void recordReceived() {
        receivedCount.incrementAndGet();
    }

    public void recordProcessed() {
        processedCount.incrementAndGet();
    }

    public void recordFiltered() {
        filteredCount.incrementAndGet();
    }

    public void recordDeduplicated() {
        deduplicatedCount.incrementAndGet();
    }

    public void recordError() {
        errorCount.incrementAndGet();
    }

    public long getReceivedCount() {
        return receivedCount.get();
    }

    public long getProcessedCount() {
        return processedCount.get();
    }

    public long getFilteredCount() {
        return filteredCount.get();
    }

    public long getDeduplicatedCount() {
        return deduplicatedCount.get();
    }

    public long getErrorCount() {
        return errorCount.get();
    }
}
