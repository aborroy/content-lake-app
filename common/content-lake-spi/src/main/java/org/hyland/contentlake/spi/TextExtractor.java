package org.hyland.contentlake.spi;

import org.springframework.core.io.Resource;

/**
 * Extracts text from a binary document.
 *
 * <p>Alfresco uses Transform Core AIO against a downloaded temp resource.
 * Nuxeo can extract text server-side from the source document/blob identity.
 * The shared sync pipeline delegates to this interface and has no knowledge
 * of the underlying transform mechanism.</p>
 *
 * <p>An implementation that can produce structure declares it through
 * {@link #preferredFormat(String)} and returns it from {@link #extract(Resource, String)}. Both
 * default to {@link TextFormat#PLAIN} on top of {@link #extractText(Resource, String)}, so an
 * implementation that only produces flattened text needs to implement nothing extra.</p>
 */
public interface TextExtractor {

    /**
     * Returns {@code true} when this extractor can process the given MIME type.
     *
     * @param mimeType source MIME type to check
     * @return {@code true} if extraction is supported
     */
    boolean supports(String mimeType);

    /**
     * Returns {@code true} when this extractor can operate directly from the source-system
     * document/blob identity instead of a downloaded temp resource.
     *
     * @param mimeType source MIME type to check
     * @return {@code true} if node-id based extraction is supported
     */
    default boolean supportsSourceReference(String mimeType) {
        return false;
    }

    /**
     * Extracts plain text by addressing the source document/blob directly.
     *
     * @param nodeId   source-system node identifier
     * @param mimeType MIME type of the source content
     * @return extracted plain text, or {@code null} when no text can be produced
     */
    default String extractText(String nodeId, String mimeType) {
        throw new UnsupportedOperationException("Source-reference extraction is not supported");
    }

    /**
     * Extracts plain text from the content resource.
     *
     * @param content  resource containing the binary content
     * @param mimeType MIME type of the source content
     * @return extracted plain text, or {@code null} when no text can be produced
     */
    String extractText(Resource content, String mimeType);

    /**
     * Representation this extractor would produce for {@code mimeType}.
     *
     * <p>Answered without doing the extraction, so the pipeline can decide how to treat the result
     * before paying for it. An implementation whose answer depends on a remote service's advertised
     * capabilities may consult it here.</p>
     *
     * @param mimeType source MIME type to check
     * @return the representation {@link #extract(Resource, String)} will return
     */
    default TextFormat preferredFormat(String mimeType) {
        return TextFormat.PLAIN;
    }

    /**
     * Extracts text and reports which representation it is in.
     *
     * @param content  resource containing the binary content
     * @param mimeType MIME type of the source content
     * @return extracted text, or {@code null} when no text can be produced
     */
    default ExtractedText extract(Resource content, String mimeType) {
        return ExtractedText.of(extractText(content, mimeType), preferredFormat(mimeType));
    }

    /**
     * Extracts text by addressing the source document/blob directly, reporting its representation.
     *
     * @param nodeId   source-system node identifier
     * @param mimeType MIME type of the source content
     * @return extracted text, or {@code null} when no text can be produced
     */
    default ExtractedText extract(String nodeId, String mimeType) {
        return ExtractedText.of(extractText(nodeId, mimeType), preferredFormat(mimeType));
    }
}
