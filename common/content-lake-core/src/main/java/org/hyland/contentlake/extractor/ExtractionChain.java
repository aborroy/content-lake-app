package org.hyland.contentlake.extractor;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.spi.TextExtractor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds the extraction chain for an ingester from a list of configured extraction services.
 *
 * <p>Several services are needed at once because no single one covers every format well. Measured on
 * the eval fixtures: {@code convert2md} recovers real markdown tables from PDF but handles PDF only,
 * while {@code liteparse} covers PDF and all the Office formats and recovers tables from spreadsheets
 * but not from PDF. Neither alone gives tables across a mixed corpus.</p>
 *
 * <p>Nothing here decides which service handles which MIME type. Each extractor is asked what it
 * supports and claims only that, so a chain routes itself: a service that cannot handle a document is
 * skipped and the next one gets its turn. Configuration controls only <em>order</em>, which breaks ties
 * when two services both claim a format. <b>List them most structural first.</b> With
 * {@code convert2md,liteparse} a PDF goes to convert2md (tables) and an XLSX falls through to liteparse
 * (tables); reverse them and liteparse claims the PDF first, yielding headings but no tables.</p>
 *
 * <h3>Entry syntax</h3>
 *
 * <p>Each entry is optionally prefixed with a backend kind, so one deployment can mix protocols:</p>
 *
 * <pre>
 * extraction.engine-urls: transform:http://convert2md:8090, docfilters:http://docfilters:8080
 * </pre>
 *
 * <p>An unprefixed entry defaults to {@link TransformCoreBackend#KIND}. A URL's own scheme is not
 * mistaken for a kind, because a prefix is only recognised when what follows the first colon does not
 * begin with {@code //}. Register further backends by passing them to
 * {@link #of(String, long, ExtractionFormat, List, TextExtractor...)}; adding one requires no change
 * here, in the pipeline, or in the {@code TextExtractor} SPI.</p>
 */
@Slf4j
public final class ExtractionChain {

    private static final List<ExtractionBackend> BUILT_IN = List.of(new TransformCoreBackend());

    private ExtractionChain() {}

    /**
     * One extractor per configured entry, in order, using the built-in backends only.
     *
     * @param engineUrls comma or whitespace delimited entries, may be {@code null} or blank
     * @param timeoutMs  read timeout applied to every service
     * @param format     representation to request
     * @return the extractors, empty when nothing is configured
     */
    public static List<TextExtractor> engines(String engineUrls, long timeoutMs, ExtractionFormat format) {
        return engines(engineUrls, timeoutMs, format, List.of());
    }

    /**
     * One extractor per configured entry, in order.
     *
     * @param extraBackends backends to recognise in addition to the built-in ones; a kind declared
     *                      here overrides a built-in of the same name
     */
    public static List<TextExtractor> engines(String engineUrls, long timeoutMs, ExtractionFormat format,
                                              List<ExtractionBackend> extraBackends) {
        Map<String, ExtractionBackend> backends = registry(extraBackends);
        List<TextExtractor> engines = new ArrayList<>();

        for (Entry entry : entries(engineUrls)) {
            ExtractionBackend backend = backends.get(entry.kind());
            if (backend == null) {
                // Skip rather than fail: one unknown kind in a list must not stop the other services,
                // and it must not stop the ingester from starting at all.
                log.warn("Ignoring extraction service {}: unknown backend kind '{}'. Known kinds: {}",
                        entry.url(), entry.kind(), backends.keySet());
                continue;
            }
            engines.add(backend.create(entry.url(), timeoutMs, format));
        }
        return engines;
    }

    /**
     * Builds the full chain: the configured services first, then {@code fallbacks} in the order given.
     *
     * <p>{@code fallbacks} is where a source's own extractor and {@link TikaTextExtractor} go. Tika
     * belongs last in every chain: it is in-process, needs nothing deployed, and is therefore the floor
     * that stops a missing or broken service from failing an ingest.</p>
     */
    public static TextExtractor of(String engineUrls, long timeoutMs, ExtractionFormat format,
                                   TextExtractor... fallbacks) {
        return of(engineUrls, timeoutMs, format, List.of(), fallbacks);
    }

    /** As {@link #of(String, long, ExtractionFormat, TextExtractor...)}, with extra backends. */
    public static TextExtractor of(String engineUrls, long timeoutMs, ExtractionFormat format,
                                   List<ExtractionBackend> extraBackends, TextExtractor... fallbacks) {
        if (fallbacks == null || fallbacks.length == 0) {
            throw new IllegalArgumentException("At least one fallback extractor is required");
        }
        List<TextExtractor> engines = engines(engineUrls, timeoutMs, format, extraBackends);
        List<TextExtractor> chain = new ArrayList<>(engines);
        chain.addAll(List.of(fallbacks));

        log.info("Extraction chain: {} configured service(s) {}, format={}, then {}",
                engines.size(), entries(engineUrls), format,
                List.of(fallbacks).stream().map(f -> f.getClass().getSimpleName()).toList());
        return new ChainingTextExtractor(chain);
    }

    /** The distinct, non-blank entries in {@code engineUrls}, in order. */
    public static List<Entry> entries(String engineUrls) {
        if (engineUrls == null || engineUrls.isBlank()) {
            return List.of();
        }
        Set<Entry> ordered = new LinkedHashSet<>();
        for (String candidate : engineUrls.split("[,\\s]+")) {
            String token = candidate.strip();
            if (!token.isEmpty()) {
                ordered.add(parse(token));
            }
        }
        return List.copyOf(ordered);
    }

    /** Convenience for callers that only want the URLs, ignoring which backend serves them. */
    public static List<String> urls(String engineUrls) {
        return entries(engineUrls).stream().map(Entry::url).toList();
    }

    private static Entry parse(String token) {
        int colon = token.indexOf(':');
        // A URL scheme is followed by "//", a kind prefix is not. That is the whole disambiguation.
        if (colon > 0 && !token.startsWith("//", colon + 1)) {
            String kind = token.substring(0, colon).strip().toLowerCase();
            String url = token.substring(colon + 1).strip();
            if (!kind.isEmpty() && !url.isEmpty()) {
                return new Entry(kind, stripTrailingSlash(url));
            }
        }
        return new Entry(TransformCoreBackend.KIND, stripTrailingSlash(token));
    }

    private static Map<String, ExtractionBackend> registry(List<ExtractionBackend> extraBackends) {
        Map<String, ExtractionBackend> backends = new LinkedHashMap<>();
        for (ExtractionBackend backend : BUILT_IN) {
            backends.put(backend.kind(), backend);
        }
        if (extraBackends != null) {
            for (ExtractionBackend backend : extraBackends) {
                if (backend != null && backend.kind() != null) {
                    backends.put(backend.kind().toLowerCase(), backend);
                }
            }
        }
        return backends;
    }

    private static String stripTrailingSlash(String url) {
        // So "http://engine:8090" and "http://engine:8090/" are recognised as the same service.
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    /**
     * One configured extraction service.
     *
     * @param kind backend kind, lowercase
     * @param url  base URL, without a trailing slash
     */
    public record Entry(String kind, String url) {
        @Override
        public String toString() {
            return kind + ":" + url;
        }
    }
}
