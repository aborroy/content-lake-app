package org.hyland.contentlake.connector.sharepoint;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Spends the tenant's Graph budget at a rate the deployment chose, and reports what it spent.
 *
 * <p>Graph meters SharePoint calls in resource units rather than requests, and the prices are not uniform:
 * 1 unit for a single-item get, a delta page with a token, or a content download; 2 for a multi-item query
 * or a delta page without one; and 5 for any permission operation. A crawl that reads permissions per
 * document therefore spends about six units per document, of which five are the ACL read, so counting
 * requests would measure the wrong thing entirely.</p>
 *
 * <p>Deliberately a token bucket rather than a request-per-second limit. The per-minute cap is what a
 * tenant enforces, and a bucket lets a burst through while holding the average under the cap, which is the
 * shape a crawl actually has: a folder listing followed by a run of downloads.</p>
 *
 * <p>The default rate is set below the documented cap on purpose. The cap is per application per tenant,
 * so anything else sharing the app registration -- a second connector instance, the query-path group
 * resolver -- draws from the same budget, and a connector that aimed exactly at the cap would push the
 * whole application into throttling rather than stopping short of it.</p>
 */
public final class ResourceUnitMeter {

    private static final Logger log = Logger.getLogger(ResourceUnitMeter.class.getName());

    private static final long NANOS_PER_MINUTE = 60_000_000_000L;

    /** Prices, from Microsoft's published table. Named so a call site reads as what it costs. */
    public static final int SINGLE_ITEM = 1;
    public static final int CONTENT_DOWNLOAD = 1;
    public static final int DELTA_WITH_TOKEN = 1;
    public static final int MULTI_ITEM_QUERY = 2;
    public static final int PERMISSIONS = 5;

    private final long unitsPerMinute;
    private final long capacity;

    /** Available units, scaled by {@link #NANOS_PER_MINUTE} so refill needs no floating point. */
    private final AtomicLong scaledAvailable;
    private final AtomicLong lastRefillNanos;

    private final AtomicLong spent = new AtomicLong();
    private final AtomicLong waitedNanos = new AtomicLong();

    /**
     * @param unitsPerMinute sustained rate to allow. Zero or negative turns metering off, which is for a
     *                       mock run where there is no budget to protect
     * @param burst          units allowed to accumulate, so a short burst is not paced to the average
     */
    public ResourceUnitMeter(int unitsPerMinute, int burst) {
        this.unitsPerMinute = Math.max(0, unitsPerMinute);
        this.capacity = Math.max(1, burst);
        this.scaledAvailable = new AtomicLong(this.capacity * NANOS_PER_MINUTE);
        this.lastRefillNanos = new AtomicLong(System.nanoTime());
    }

    /** Metering off: for the mock, and for a tenant whose budget the operator is managing elsewhere. */
    public static ResourceUnitMeter unmetered() {
        return new ResourceUnitMeter(0, 1);
    }

    /** Whether this meter paces anything. */
    public boolean enabled() {
        return unitsPerMinute > 0;
    }

    /**
     * Waits until {@code units} are available, then spends them.
     *
     * <p>Blocking is the point: the alternative is a queue that grows until the heap does, or a rejection
     * the caller has to invent a policy for. A crawl has no deadline, so waiting is free where being
     * throttled is not, and every unit spent while throttled still counts against the cap.</p>
     */
    public void spend(int units) {
        spent.addAndGet(units);
        if (!enabled() || units <= 0) {
            return;
        }

        // Capped at the bucket's own size: a call that costs more units than the bucket can ever hold
        // would otherwise wait for an amount that never becomes available, which is a hang rather than a
        // slow crawl. Waiting for a full bucket and proceeding is the honest reading of "as slow as this
        // meter can go".
        long need = Math.min((long) units * NANOS_PER_MINUTE, capacity * NANOS_PER_MINUTE);
        while (true) {
            refill();
            long available = scaledAvailable.get();
            if (available >= need && scaledAvailable.compareAndSet(available, available - need)) {
                return;
            }
            if (available < need) {
                // Time for the shortfall to refill, at unitsPerMinute per minute.
                long shortfallNanos = (need - available) / unitsPerMinute;
                sleep(Math.max(1_000_000L, Math.min(shortfallNanos, NANOS_PER_MINUTE)));
            }
        }
    }

    private void refill() {
        long now = System.nanoTime();
        long last = lastRefillNanos.get();
        long elapsed = now - last;
        if (elapsed <= 0 || !lastRefillNanos.compareAndSet(last, now)) {
            return;
        }
        long refill = elapsed * unitsPerMinute;
        long ceiling = capacity * NANOS_PER_MINUTE;
        scaledAvailable.updateAndGet(current -> Math.min(ceiling, current + refill));
    }

    private void sleep(long nanos) {
        waitedNanos.addAndGet(nanos);
        try {
            Thread.sleep(nanos / 1_000_000L, (int) (nanos % 1_000_000L));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GraphException("Interrupted while waiting for Graph resource units", e);
        }
    }

    /** Total units spent, which is what makes the cost per document measurable instead of estimated. */
    public long unitsSpent() {
        return spent.get();
    }

    /** How long callers waited on the budget, so pacing is distinguishable from a slow tenant. */
    public long waitedMillis() {
        return waitedNanos.get() / 1_000_000L;
    }

    /** One line for the end of a run, where the cost per document is worth stating. */
    public void logSummary(String context, long documents) {
        if (documents <= 0) {
            log.info(() -> context + ": spent " + unitsSpent() + " Graph resource units, waited "
                    + waitedMillis() + " ms on the budget");
            return;
        }
        String perDocument = String.format("%.2f", (double) unitsSpent() / documents);
        log.info(() -> context + ": spent " + unitsSpent() + " Graph resource units over " + documents
                + " document(s), " + perDocument + " per document, waited " + waitedMillis()
                + " ms on the budget");
    }
}
