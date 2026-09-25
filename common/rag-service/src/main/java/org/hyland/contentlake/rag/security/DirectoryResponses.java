package org.hyland.contentlake.rag.security;

/** Shared parsing of the loosely typed JSON bodies the source directories answer with. */
final class DirectoryResponses {

    private DirectoryResponses() {
    }

    /** The first value that is a non-blank string, or {@code null}. */
    static String firstString(Object... values) {
        for (Object value : values) {
            if (value instanceof String text && !text.isBlank()) {
                return text;
            }
        }
        return null;
    }
}
