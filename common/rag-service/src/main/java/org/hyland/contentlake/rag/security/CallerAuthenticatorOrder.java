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
    /**
     * After both, so an existing deployment's validation order is untouched and a CMIS repository is only
     * shown a caller's password once Alfresco and Nuxeo have declined it.
     */
    static final int CMIS = 30;
    /**
     * Last, and immaterial. The bearer authenticator is the only one claiming a credential that carries a token
     * attribute, and it is the only credential shape it claims, so its position cannot change an outcome. Given
     * a value at all because an unordered chain is a chain whose order nobody has decided.
     */
    static final int ENTRA = 40;

    private CallerAuthenticatorOrder() {
    }
}
