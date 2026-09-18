package org.hyland.contentlake.connector.sharepoint;

/**
 * A Graph call, or the token acquisition in front of it, failed in a way the connector cannot paper over.
 *
 * <p>Deliberately unchecked and deliberately not used for throttling: a 429 is handled inside
 * {@link GraphHttpClient} by waiting, and only a request that has exhausted its retries becomes one of
 * these. It is also not used for a stale change-feed cursor, which the SPI models as
 * {@code SourceChangePage.expired()} rather than as a failure.</p>
 */
public class GraphException extends RuntimeException {

    private final int statusCode;

    public GraphException(String message) {
        this(message, 0, null);
    }

    public GraphException(String message, Throwable cause) {
        this(message, 0, cause);
    }

    public GraphException(String message, int statusCode, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    /** HTTP status that caused this, or {@code 0} when it did not come from a response. */
    public int statusCode() {
        return statusCode;
    }
}
