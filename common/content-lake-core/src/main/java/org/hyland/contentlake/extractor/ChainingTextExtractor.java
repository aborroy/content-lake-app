package org.hyland.contentlake.extractor;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.spi.ExtractedText;
import org.hyland.contentlake.spi.TextExtractor;
import org.hyland.contentlake.spi.TextFormat;
import org.springframework.core.io.Resource;

import java.util.List;
import java.util.Optional;

/**
 * Tries extractors in order and uses the first that produces text.
 *
 * <p>The point is that extraction degrades instead of failing. A transform engine may be absent from
 * a deployment, slow to start, out of memory or simply unable to handle a format; in every one of
 * those cases the document still needs to be indexed, with whatever text can be recovered. Placing a
 * remote engine first and {@link TikaTextExtractor} last gives structure when it is available and
 * in-process extraction when it is not.</p>
 *
 * <p>Falling through means "produced no text": a {@code null} return, or a
 * {@link RuntimeException}. An exception from one extractor is logged and swallowed so the next gets
 * its turn; only when every extractor has failed does the chain return {@code null}, which the
 * pipeline records as a per-document extraction failure rather than an aborted sync.</p>
 *
 * <p>Ordering is the caller's, and it is significant. A chain is normally
 * {@code [TransformEngineTextExtractor, source-specific extractor, TikaTextExtractor]}.</p>
 */
@Slf4j
public class ChainingTextExtractor implements TextExtractor {

    private final List<TextExtractor> delegates;

    public ChainingTextExtractor(TextExtractor... delegates) {
        this(List.of(delegates));
    }

    public ChainingTextExtractor(List<TextExtractor> delegates) {
        if (delegates == null || delegates.isEmpty()) {
            throw new IllegalArgumentException("At least one delegate extractor is required");
        }
        if (delegates.stream().anyMatch(d -> d == null)) {
            throw new IllegalArgumentException("Delegate extractors must not be null");
        }
        this.delegates = List.copyOf(delegates);
        log.info("ChainingTextExtractor initialized with {}",
                this.delegates.stream().map(d -> d.getClass().getSimpleName()).toList());
    }

    /** True when any delegate claims the MIME type. */
    @Override
    public boolean supports(String mimeType) {
        return delegates.stream().anyMatch(d -> supportsQuietly(d, mimeType));
    }

    /**
     * True only when the <em>first</em> delegate claiming this MIME type wants the source reference.
     *
     * <p>The two extraction styles cannot be mixed within one document: a source-reference extractor
     * is handed a node id and a resource-based one a downloaded temp file, and the pipeline picks
     * which to call before any extractor runs. Answering for the first claiming delegate keeps that
     * decision consistent with which delegate the chain will actually reach first.</p>
     */
    @Override
    public boolean supportsSourceReference(String mimeType) {
        return firstClaiming(mimeType).map(d -> supportsSourceReferenceQuietly(d, mimeType)).orElse(false);
    }

    @Override
    public TextFormat preferredFormat(String mimeType) {
        return firstClaiming(mimeType)
                .map(d -> preferredFormatQuietly(d, mimeType))
                .orElse(TextFormat.PLAIN);
    }

    @Override
    public String extractText(Resource content, String mimeType) {
        ExtractedText extracted = extract(content, mimeType);
        return extracted == null ? null : extracted.text();
    }

    @Override
    public String extractText(String nodeId, String mimeType) {
        ExtractedText extracted = extract(nodeId, mimeType);
        return extracted == null ? null : extracted.text();
    }

    @Override
    public ExtractedText extract(Resource content, String mimeType) {
        return attempt(mimeType, delegate -> delegate.extract(content, mimeType), false);
    }

    @Override
    public ExtractedText extract(String nodeId, String mimeType) {
        return attempt(mimeType, delegate -> delegate.extract(nodeId, mimeType), true);
    }

    /**
     * Walks the chain until one delegate yields text.
     *
     * @param sourceReference when true, only delegates that support source-reference extraction are
     *                        tried, because the others cannot be handed a node id
     */
    private ExtractedText attempt(String mimeType, Extraction extraction, boolean sourceReference) {
        for (TextExtractor delegate : delegates) {
            String name = delegate.getClass().getSimpleName();
            if (!supportsQuietly(delegate, mimeType)) {
                continue;
            }
            if (sourceReference && !supportsSourceReferenceQuietly(delegate, mimeType)) {
                continue;
            }
            try {
                ExtractedText extracted = extraction.apply(delegate);
                if (extracted != null && extracted.text() != null && !extracted.text().isBlank()) {
                    return extracted;
                }
                log.info("{} produced no text for {}, trying the next extractor", name, mimeType);
            } catch (RuntimeException e) {
                log.warn("{} failed on {} ({}), trying the next extractor", name, mimeType,
                        e.getMessage());
            }
        }
        log.info("No extractor produced text for {}", mimeType);
        return null;
    }

    private Optional<TextExtractor> firstClaiming(String mimeType) {
        return delegates.stream().filter(d -> supportsQuietly(d, mimeType)).findFirst();
    }

    // A delegate that throws while merely answering a capability question must not take the chain
    // down with it: an unreachable engine can fail on its config lookup as easily as on a transform.

    private boolean supportsQuietly(TextExtractor delegate, String mimeType) {
        try {
            return delegate.supports(mimeType);
        } catch (RuntimeException e) {
            log.warn("{} failed answering supports({}): {}", delegate.getClass().getSimpleName(),
                    mimeType, e.getMessage());
            return false;
        }
    }

    private boolean supportsSourceReferenceQuietly(TextExtractor delegate, String mimeType) {
        try {
            return delegate.supportsSourceReference(mimeType);
        } catch (RuntimeException e) {
            return false;
        }
    }

    private TextFormat preferredFormatQuietly(TextExtractor delegate, String mimeType) {
        try {
            return delegate.preferredFormat(mimeType);
        } catch (RuntimeException e) {
            return TextFormat.PLAIN;
        }
    }

    @FunctionalInterface
    private interface Extraction {
        ExtractedText apply(TextExtractor delegate);
    }
}
