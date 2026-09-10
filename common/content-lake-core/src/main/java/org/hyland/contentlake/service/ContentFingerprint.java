package org.hyland.contentlake.service;

import org.hyland.contentlake.service.chunking.strategy.ChunkingStrategy.ChunkingConfig;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Fingerprint of everything that determines a document's stored chunks and vectors.
 *
 * <p>Stored per document so a sync can tell that reprocessing would produce byte-identical
 * embeddings and skip chunking and embedding entirely. Embedding throughput is the pipeline
 * bottleneck, and a permission change, a property edit or a folder move all move the modification
 * date without changing a single byte of content.</p>
 *
 * <h3>Why this covers more than the text</h3>
 * <p>A fingerprint over the extracted text alone is a trap. Change the configured embedding model
 * and re-sync, and every document would match its stored fingerprint, short-circuit, and leave the
 * corpus serving the retired model's vectors while the type-aware clear path never runs. The
 * embedding type and the chunking parameters are therefore part of the input: a change to either
 * legitimately invalidates the stored vectors and must force a reprocess.</p>
 *
 * <p>The keyword-enrichment flag is included for the same reason one level down: it changes the
 * extracted-text mirror the keyword leg matches against, so toggling it has to rewrite that mirror.</p>
 */
public final class ContentFingerprint {

    /**
     * Version of the fingerprint recipe, and the first field of every hashed input.
     *
     * <p>Bumping it invalidates every stored fingerprint, which is the point: a change to what the
     * fingerprint covers must force one reprocess per document rather than silently keeping stale
     * vectors that the new recipe would no longer accept.</p>
     */
    static final String RECIPE_VERSION = "v1";

    private static final String ALGORITHM = "SHA-256";

    /** Prefix on the stored value, so the algorithm is readable from the indexed property. */
    private static final String PREFIX = "sha256:";

    private static final char FIELD_SEPARATOR = '\n';

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private ContentFingerprint() {
    }

    /**
     * Computes the fingerprint of an extracted document under a given pipeline configuration.
     *
     * @param extractedText            text produced by extraction, before chunking
     * @param embeddingType            the hxpr embedding type the vectors are written under
     *                                 ({@code HxprService.getEmbeddingType()}, not the raw model name)
     * @param chunkingConfig           the chunking parameters in force
     * @param keywordContextEnrichment whether the document-context prefix is prepended to the
     *                                 keyword-leg mirror
     * @return the fingerprint, prefixed with its algorithm
     * @throws IllegalArgumentException when {@code extractedText} is null
     */
    public static String of(String extractedText,
                            String embeddingType,
                            ChunkingConfig chunkingConfig,
                            boolean keywordContextEnrichment) {
        if (extractedText == null) {
            throw new IllegalArgumentException("Cannot fingerprint null extracted text");
        }

        StringBuilder input = new StringBuilder();
        input.append(RECIPE_VERSION).append(FIELD_SEPARATOR)
                .append(embeddingType == null ? "" : embeddingType).append(FIELD_SEPARATOR)
                .append(describe(chunkingConfig)).append(FIELD_SEPARATOR)
                .append(keywordContextEnrichment ? '1' : '0').append(FIELD_SEPARATOR)
                .append(extractedText);

        return PREFIX + hex(digest(input.toString()));
    }

    /**
     * Whether a stored fingerprint matches a freshly computed one. Null or blank never matches: a
     * document with no stored fingerprint has to be reprocessed to acquire one.
     */
    public static boolean matches(String stored, String computed) {
        return stored != null && !stored.isBlank() && stored.equals(computed);
    }

    private static String describe(ChunkingConfig config) {
        if (config == null) {
            return "";
        }
        return config.minChunkSize() + "/" + config.maxChunkSize() + "/"
                + config.overlapSize() + "/" + config.similarityThreshold();
    }

    private static byte[] digest(String input) {
        try {
            return MessageDigest.getInstance(ALGORITHM).digest(input.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the platform, so this cannot happen on a working JVM.
            throw new IllegalStateException(ALGORITHM + " is not available", e);
        }
    }

    private static String hex(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xFF;
            out[i * 2] = HEX[b >>> 4];
            out[i * 2 + 1] = HEX[b & 0x0F];
        }
        return new String(out);
    }
}
