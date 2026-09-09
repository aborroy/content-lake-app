package org.hyland.contentlake.extractor;

import org.hyland.contentlake.spi.TextExtractor;

/**
 * Builds a {@link TextExtractor} for a remote extraction service of a particular kind.
 *
 * <p>This exists so that "a remote extraction service at a URL" does not mean "something speaking the
 * {@code alfresco-transform-core} protocol". That protocol is one option, not the model. Document
 * Filters, a cloud extraction API, or anything else can be added by implementing this interface and
 * registering it, with no change to the chain, the pipeline or the {@link TextExtractor} SPI.</p>
 *
 * <p>A backend is selected per configured entry by a {@code kind:} prefix, so a single deployment can
 * mix them:</p>
 *
 * <pre>
 * extraction.engine-urls: transform:http://convert2md:8090, docfilters:http://docfilters:8080
 * </pre>
 *
 * <p>An entry with no prefix defaults to {@link TransformCoreBackend#KIND}, which is what every
 * existing deployment has configured.</p>
 *
 * <p>Implementations must be cheap to construct and must not perform I/O in
 * {@link #create(String, long, ExtractionFormat)}: the chain is built at startup, and a backend whose
 * service is not yet reachable has to yield a working extractor that degrades at call time rather than
 * failing the context. {@link TextExtractor} already specifies that contract: return {@code null} when
 * no text can be produced, and let the chain fall through.</p>
 */
public interface ExtractionBackend {

    /**
     * Configuration token that selects this backend, lowercase, no colon.
     *
     * @return the kind, for example {@code transform} or {@code docfilters}
     */
    String kind();

    /**
     * Creates an extractor for one service instance.
     *
     * @param url       base URL of the service
     * @param timeoutMs read timeout for a single extraction
     * @param format    representation the pipeline would prefer. A backend that cannot produce
     *                  markdown ignores this and reports {@code PLAIN} from
     *                  {@link TextExtractor#preferredFormat(String)}
     * @return an extractor, never {@code null}
     */
    TextExtractor create(String url, long timeoutMs, ExtractionFormat format);
}
