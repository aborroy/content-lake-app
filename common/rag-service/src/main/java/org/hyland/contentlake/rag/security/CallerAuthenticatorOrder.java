package org.hyland.contentlake.rag.security;

/**
 * Where each credential authority sits in the chain.
 *
 * <p>Named constants rather than literals because registration order is a security property: it decides which
 * system is shown a caller's credentials first. The values are spaced so a new authority can be inserted
 * without renumbering, and the existing two keep the order the fixed {@code if} chain validated in, so no
 * established deployment's credentials start going somewhere new.</p>
 */
final class CallerAuthenticatorOrder {

    static final int ALFRESCO = 10;
    static final int NUXEO = 20;

    private CallerAuthenticatorOrder() {
    }
}
