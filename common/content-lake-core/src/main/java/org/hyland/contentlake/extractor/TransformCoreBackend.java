package org.hyland.contentlake.extractor;

import org.hyland.contentlake.spi.TextExtractor;

/**
 * {@link ExtractionBackend} for services speaking the {@code alfresco-transform-core} HTTP contract:
 * multipart {@code POST /transform} plus capability discovery over {@code GET /transform/config}.
 *
 * <p>This is the default kind, so an entry with no {@code kind:} prefix resolves here and every
 * existing configuration keeps working unchanged.</p>
 */
public final class TransformCoreBackend implements ExtractionBackend {

    /** Configuration token for this backend, and the default when an entry carries no prefix. */
    public static final String KIND = "transform";

    @Override
    public String kind() {
        return KIND;
    }

    @Override
    public TextExtractor create(String url, long timeoutMs, ExtractionFormat format) {
        return new TransformEngineTextExtractor(url, timeoutMs, format);
    }
}
