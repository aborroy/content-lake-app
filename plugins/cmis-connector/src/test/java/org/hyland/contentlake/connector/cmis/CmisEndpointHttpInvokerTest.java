package org.hyland.contentlake.connector.cmis;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sending every CMIS call to the endpoint that was configured.
 *
 * <p>The case that made this necessary, measured against the Alfresco in the deployment stack: the
 * connector connects to {@code http://alfresco:8080/...}, the service document advertises
 * {@code http://localhost:80/...}, and the next call dials the ingester container itself.</p>
 */
class CmisEndpointHttpInvokerTest {

    private static final String ENDPOINT =
            "http://alfresco:8080/alfresco/api/-default-/public/cmis/versions/1.1/browser";

    @Test
    void rewritesTheHostAndPortTheRepositoryAdvertised() {
        String rewritten = CmisEndpointHttpInvoker.rewriteAuthority(
                "http://localhost:80/alfresco/api/-default-/public/cmis/versions/1.1/browser/root"
                        + "?objectId=abc&cmisselector=object",
                ENDPOINT);

        assertThat(rewritten).isEqualTo(
                "http://alfresco:8080/alfresco/api/-default-/public/cmis/versions/1.1/browser/root"
                        + "?objectId=abc&cmisselector=object");
    }

    @Test
    void keepsThePathAndQueryTheRepositoryChose() {
        // The path is the repository's own routing, and a repository may advertise a different one than
        // the entry point. Only the authority is ours to correct.
        String rewritten = CmisEndpointHttpInvoker.rewriteAuthority(
                "http://localhost/some/other/prefix/root?cmisselector=children&maxItems=50", ENDPOINT);

        assertThat(rewritten).isEqualTo(
                "http://alfresco:8080/some/other/prefix/root?cmisselector=children&maxItems=50");
    }

    @Test
    void leavesAUrlAloneWhenItAlreadyPointsAtTheConfiguredEndpoint() {
        // Null means "send what OpenCMIS built", so the common case costs no object churn and no risk.
        assertThat(CmisEndpointHttpInvoker.rewriteAuthority(ENDPOINT + "/root?cmisselector=object", ENDPOINT))
                .isNull();
    }

    @Test
    void treatsADefaultedPortAsEqualToTheExplicitOne() {
        assertThat(CmisEndpointHttpInvoker.rewriteAuthority(
                "http://cmis.example.com:80/browser/root", "http://cmis.example.com/browser")).isNull();
        assertThat(CmisEndpointHttpInvoker.rewriteAuthority(
                "https://cmis.example.com:443/browser/root", "https://cmis.example.com/browser")).isNull();
    }

    @Test
    void rewritesTheSchemeToo() {
        // A repository behind a TLS-terminating proxy advertises http while the client speaks https.
        assertThat(CmisEndpointHttpInvoker.rewriteAuthority(
                "http://cmis.example.com/browser/root", "https://cmis.example.com/browser"))
                .isEqualTo("https://cmis.example.com/browser/root");
    }

    @Test
    void doesNothingWithoutBothUrls() {
        assertThat(CmisEndpointHttpInvoker.rewriteAuthority(null, ENDPOINT)).isNull();
        assertThat(CmisEndpointHttpInvoker.rewriteAuthority("http://localhost/x", null)).isNull();
        assertThat(CmisEndpointHttpInvoker.rewriteAuthority("http://localhost/x", "  ")).isNull();
    }

    @Test
    void doesNotRewriteAUrlItCannotParse() {
        assertThat(CmisEndpointHttpInvoker.rewriteAuthority("not a url at all", ENDPOINT)).isNull();
    }
}
