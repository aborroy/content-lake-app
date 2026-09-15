package org.hyland.contentlake.connector.cmis;

/**
 * The connection settings a {@link CmisConnectorClient} is built from.
 *
 * <p>Separate from the plugin so the client can be constructed in a test without a
 * {@link org.hyland.contentlake.spi.ConnectorContext}, and so the property names live in exactly one
 * place ({@link CmisConnectorPlugin}).</p>
 *
 * @param url          CMIS service endpoint
 * @param binding      which CMIS binding to speak
 * @param repositoryId repository to open, or {@code null} to resolve it when the endpoint exposes one
 * @param username     account to authenticate as
 * @param password     that account's password
 * @param rootPath     folder path a batch pass starts from, or {@code null} for the repository root
 * @param sourceId     alias stored in {@code cin_sourceId}, or {@code null} to use the repository id
 * @param pageSize     children requested per folder listing
 * @param aclFallback  what to do when the repository cannot report ACLs
 */
public record CmisConnectorSettings(
        String url,
        Binding binding,
        String repositoryId,
        String username,
        String password,
        String rootPath,
        String sourceId,
        int pageSize,
        CmisAclMapper.AclFallback aclFallback
) {

    public CmisConnectorSettings {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException(CmisConnectorPlugin.URL_SETTING + " is required");
        }
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException(CmisConnectorPlugin.USERNAME_SETTING + " is required");
        }
        if (password == null || password.isBlank()) {
            throw new IllegalArgumentException(CmisConnectorPlugin.PASSWORD_SETTING + " is required");
        }
        binding = binding == null ? Binding.BROWSER : binding;
        aclFallback = aclFallback == null ? CmisAclMapper.AclFallback.FAIL_CLOSED : aclFallback;
        pageSize = pageSize > 0 ? pageSize : 100;
    }

    /**
     * The CMIS binding to speak.
     *
     * <p>Browser is the default because it is JSON over HTTP, is what CMIS 1.1 repositories expose, and
     * needs no SOAP stack. AtomPub is kept for a repository that offers only CMIS 1.0. The Web Services
     * binding is deliberately absent: it would pull a JAX-WS stack into a plugin jar for repositories
     * that all also speak one of these two.</p>
     */
    public enum Binding {
        BROWSER,
        ATOMPUB
    }
}
