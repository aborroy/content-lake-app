package org.hyland.contentlake.spi;

import org.springframework.core.io.Resource;

/**
 * Extracts plain text from a binary document.
 *
 * <p>Alfresco uses Transform Core AIO against a downloaded temp resource.
 * Nuxeo can extract text server-side from the source document/blob identity.
 * The shared sync pipeline delegates to this interface and has no knowledge
 * of the underlying transform mechanism.</p>
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
}
