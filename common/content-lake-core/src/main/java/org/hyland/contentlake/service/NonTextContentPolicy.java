package org.hyland.contentlake.service;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Which content cannot contain text, and is therefore not worth downloading.
 *
 * <p>The pipeline used to attempt extraction on anything the host's extractor chain would accept, which on a
 * real corpus means every signature sidecar, certificate, key store, archive, disk image and media file is
 * downloaded and handed to a parser that can only fail. On a metered source that download is the scarcest
 * resource there is: a SharePoint crawl pays a Graph resource unit for each one. A detached signature file in a
 * real OneDrive folder is also what ended a crawl at 51 of 52 documents, because the parser threw something
 * outside {@code Exception}.</p>
 *
 * <p><strong>A deny list, not an allow list.</strong> An allow list silently drops formats the transform
 * services could have handled, which is the failure mode that costs a customer their content. A deny list only
 * ever skips what it names, so being wrong about a type means one missed skip rather than one missed
 * document.</p>
 *
 * <p><strong>Extension as well as MIME type,</strong> because the types that matter most are invisible to a MIME
 * check. Graph reports a {@code .csig} detached signature as {@code application/octet-stream}, and denying that
 * whole type would skip every legitimate file a source could not identify. The extension is the only thing that
 * distinguishes them.</p>
 */
public record NonTextContentPolicy(Set<String> deniedMimeTypes, Set<String> deniedExtensions) {

    /**
     * Types whose bytes are a signature, a key, a container or a rendering, never prose.
     *
     * <p>Archives are here because the pipeline has no unpacking step: an extractor handed a zip yields either
     * nothing or its filenames, and neither is the document's text. Media is here for the same reason -- there
     * is no transcription step, so an audio file's extraction cannot succeed, only cost.</p>
     */
    private static final Set<String> DEFAULT_MIME_TYPES = Set.of(
            "application/pkcs7-signature", "application/pkcs7-mime", "application/pkcs8",
            "application/pkcs12", "application/x-pkcs12", "application/x-pkcs7-certificates",
            "application/x-x509-ca-cert", "application/x-x509-user-cert",
            "application/zip", "application/x-zip-compressed", "application/x-tar",
            "application/gzip", "application/x-gzip", "application/x-bzip2",
            "application/x-7z-compressed", "application/vnd.rar", "application/x-rar-compressed",
            "application/x-iso9660-image", "application/x-apple-diskimage",
            "application/x-msdownload", "application/x-sharedlib", "application/x-executable",
            "application/x-font-ttf", "font/ttf", "font/otf", "font/woff", "font/woff2"
    );

    /**
     * Extensions that decide the question when the MIME type does not.
     *
     * <p>Stored without the leading dot and compared lower-case. Deliberately narrow: only types whose whole
     * purpose is to carry bytes that are not prose, so nothing here can plausibly hold a document.</p>
     */
    private static final Set<String> DEFAULT_EXTENSIONS = Set.of(
            "csig", "p7s", "p7m", "p7b", "pfx", "p12", "cer", "crt", "der", "key", "pem",
            "jks", "keystore", "truststore",
            "zip", "tar", "gz", "tgz", "bz2", "xz", "7z", "rar",
            "iso", "dmg", "vmdk", "vhd",
            "exe", "dll", "so", "dylib", "bin", "o", "a", "class", "jar",
            "ttf", "otf", "woff", "woff2", "eot",
            "mp3", "wav", "flac", "ogg", "m4a", "aac",
            "mp4", "avi", "mkv", "mov", "wmv", "webm",
            "psd", "ai", "sketch", "blend", "fbx", "stl"
    );

    /** The list every source gets unless a deployment names its own. */
    public static NonTextContentPolicy defaults() {
        return new NonTextContentPolicy(DEFAULT_MIME_TYPES, DEFAULT_EXTENSIONS);
    }

    /**
     * A policy that denies nothing, for a deployment that would rather pay than risk a wrong skip.
     *
     * <p>This is what an empty configured list means, and it restores the behaviour that existed before this
     * policy: everything is downloaded and offered to the extractor.</p>
     */
    public static NonTextContentPolicy allowEverything() {
        return new NonTextContentPolicy(Set.of(), Set.of());
    }

    /**
     * Builds a policy from configured values, normalising as it goes.
     *
     * <p>Both lists are normalised rather than trusted: a MIME type is lower-cased and stripped of any
     * {@code ;charset=} parameter, and an extension is lower-cased and stripped of a leading dot, so
     * {@code .PDF}, {@code PDF} and {@code pdf} are one entry. Without that, a configuration that looks right
     * silently matches nothing.</p>
     */
    public static NonTextContentPolicy of(Collection<String> mimeTypes, Collection<String> extensions) {
        return new NonTextContentPolicy(
                normalise(mimeTypes, NonTextContentPolicy::normaliseMimeType),
                normalise(extensions, NonTextContentPolicy::normaliseExtension));
    }

    public NonTextContentPolicy {
        deniedMimeTypes = deniedMimeTypes == null ? Set.of() : Set.copyOf(deniedMimeTypes);
        deniedExtensions = deniedExtensions == null ? Set.of() : Set.copyOf(deniedExtensions);
    }

    /**
     * Whether this content can be skipped without downloading it.
     *
     * <p>Both inputs are optional, because a source may report one and not the other. A node with neither is
     * not denied: "we know nothing about it" is not the same claim as "it cannot contain text", and guessing
     * in that direction loses documents.</p>
     *
     * @param mimeType     the type the source reported, or {@code null}
     * @param documentName the file name the source reported, or {@code null}
     */
    public boolean cannotContainText(String mimeType, String documentName) {
        return deniesMimeType(mimeType) || deniesExtensionOf(documentName);
    }

    /** Which of the two rules matched, for a log line that says why rather than only that. */
    public String reasonFor(String mimeType, String documentName) {
        if (deniesMimeType(mimeType)) {
            return "its type " + normaliseMimeType(mimeType) + " cannot contain text";
        }
        if (deniesExtensionOf(documentName)) {
            return "its extension ." + extensionOf(documentName) + " cannot contain text";
        }
        return null;
    }

    private boolean deniesMimeType(String mimeType) {
        String normalised = normaliseMimeType(mimeType);
        return normalised != null && deniedMimeTypes.contains(normalised);
    }

    private boolean deniesExtensionOf(String documentName) {
        String extension = extensionOf(documentName);
        return extension != null && deniedExtensions.contains(extension);
    }

    /**
     * The extension of a file name, or {@code null} when it has none worth testing.
     *
     * <p>The last dot wins, so {@code Multilanguage.docx_signed.csig} yields {@code csig} rather than
     * {@code docx_signed.csig}, which is the case that made this policy necessary. A leading-dot name such as
     * {@code .gitignore} has no extension, and neither does a trailing dot.</p>
     */
    private static String extensionOf(String documentName) {
        if (documentName == null || documentName.isBlank()) {
            return null;
        }
        String name = documentName.trim();
        int dot = name.lastIndexOf('.');
        if (dot <= 0 || dot == name.length() - 1) {
            return null;
        }
        return name.substring(dot + 1).toLowerCase(Locale.ROOT);
    }

    private static String normaliseMimeType(String mimeType) {
        if (mimeType == null || mimeType.isBlank()) {
            return null;
        }
        String normalised = mimeType.trim().toLowerCase(Locale.ROOT);
        int parameter = normalised.indexOf(';');
        return parameter < 0 ? normalised : normalised.substring(0, parameter).trim();
    }

    private static String normaliseExtension(String extension) {
        if (extension == null || extension.isBlank()) {
            return null;
        }
        String normalised = extension.trim().toLowerCase(Locale.ROOT);
        while (normalised.startsWith(".")) {
            normalised = normalised.substring(1);
        }
        return normalised.isEmpty() ? null : normalised;
    }

    private static Set<String> normalise(Collection<String> values,
                                         java.util.function.UnaryOperator<String> normaliser) {
        if (values == null) {
            return Set.of();
        }
        Set<String> normalised = new LinkedHashSet<>();
        for (String value : values) {
            String one = normaliser.apply(value);
            if (one != null) {
                normalised.add(one);
            }
        }
        return Set.copyOf(normalised);
    }
}
