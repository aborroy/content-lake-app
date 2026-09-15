package org.hyland.contentlake.connector.cmis;

import org.apache.chemistry.opencmis.client.bindings.spi.BindingSession;
import org.apache.chemistry.opencmis.client.bindings.spi.http.DefaultHttpInvoker;
import org.apache.chemistry.opencmis.client.bindings.spi.http.HttpInvoker;
import org.apache.chemistry.opencmis.client.bindings.spi.http.Output;
import org.apache.chemistry.opencmis.client.bindings.spi.http.Response;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.impl.UrlBuilder;

import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sends every CMIS request to the endpoint the deployment configured, not the one the repository says it
 * is reachable at.
 *
 * <p>A CMIS client makes one call to the configured URL and then follows the URLs in the response: for
 * the browser binding, the service document's {@code repositoryUrl} and {@code rootFolderUrl}. Those are
 * rendered by the repository from its own idea of its hostname, which is routinely not the client's route
 * to it. Measured against the Alfresco in this stack: the connector connects to
 * {@code http://alfresco:8080/...}, the service document comes back advertising
 * {@code http://localhost:80/...}, and the very next call, {@code getRootFolder}, dials the ingester
 * container's own port 80 and fails with connection refused. The session opens, the repository info
 * arrives, and then nothing works, which is a confusing shape of failure.</p>
 *
 * <p>This makes the operator's URL authoritative. Only the scheme, host and port are replaced; the path
 * and query are the repository's own and are left untouched, so a repository that advertises a different
 * <em>path</em> still works. There is no way to ask OpenCMIS to do this, hence a whole
 * {@link HttpInvoker}: it is instantiated by class name through
 * {@link SessionParameter#HTTP_INVOKER_CLASS}, so it must keep its public no-argument constructor, and it
 * reads the configured endpoint from the session rather than being given it.</p>
 *
 * <p>The alternative would be reconfiguring the repository to advertise a reachable host, which is a
 * change to a shared system that also affects its browser-facing links. A connector should not need one.</p>
 */
public class CmisEndpointHttpInvoker implements HttpInvoker {

    private static final Logger log = Logger.getLogger(CmisEndpointHttpInvoker.class.getName());

    private final DefaultHttpInvoker delegate = new DefaultHttpInvoker();

    @Override
    public Response invokeGET(UrlBuilder url, BindingSession session) {
        return delegate.invokeGET(rewrite(url, session), session);
    }

    @Override
    public Response invokeGET(UrlBuilder url, BindingSession session, BigInteger offset, BigInteger length) {
        return delegate.invokeGET(rewrite(url, session), session, offset, length);
    }

    @Override
    public Response invokePOST(UrlBuilder url, String contentType, Output writer, BindingSession session) {
        return delegate.invokePOST(rewrite(url, session), contentType, writer, session);
    }

    @Override
    public Response invokePUT(UrlBuilder url, String contentType, Map<String, String> headers,
                              Output writer, BindingSession session) {
        return delegate.invokePUT(rewrite(url, session), contentType, headers, writer, session);
    }

    @Override
    public Response invokeDELETE(UrlBuilder url, BindingSession session) {
        return delegate.invokeDELETE(rewrite(url, session), session);
    }

    private UrlBuilder rewrite(UrlBuilder url, BindingSession session) {
        String rewritten = rewriteAuthority(url == null ? null : url.toString(), configuredEndpoint(session));
        return rewritten == null ? url : new UrlBuilder(rewritten);
    }

    private static String configuredEndpoint(BindingSession session) {
        if (session == null) {
            return null;
        }
        Object browser = session.get(SessionParameter.BROWSER_URL);
        if (browser instanceof String value && !value.isBlank()) {
            return value;
        }
        Object atompub = session.get(SessionParameter.ATOMPUB_URL);
        return atompub instanceof String value && !value.isBlank() ? value : null;
    }

    /**
     * {@code requestUrl} with the scheme, host and port of {@code configuredEndpoint}.
     *
     * @return the rewritten URL, or {@code null} when either input cannot be parsed or nothing needs
     *         changing, which tells the caller to send the request exactly as OpenCMIS built it
     */
    static String rewriteAuthority(String requestUrl, String configuredEndpoint) {
        if (requestUrl == null || requestUrl.isBlank() || configuredEndpoint == null || configuredEndpoint.isBlank()) {
            return null;
        }
        try {
            URI request = new URI(requestUrl);
            URI configured = new URI(configuredEndpoint);
            if (request.getHost() == null || configured.getHost() == null) {
                return null;
            }
            if (sameAuthority(request, configured)) {
                return null;
            }

            URI result = new URI(configured.getScheme(), null, configured.getHost(), configured.getPort(),
                    request.getPath(), request.getQuery(), request.getFragment());
            log.log(Level.FINE, () -> "Rewrote CMIS request host: " + request.getHost() + " -> "
                    + configured.getHost());
            return result.toString();
        } catch (URISyntaxException e) {
            // A URL neither side can parse is not one to rewrite: let OpenCMIS send what it built and
            // fail with its own message rather than one from here.
            return null;
        }
    }

    private static boolean sameAuthority(URI request, URI configured) {
        return request.getHost().equalsIgnoreCase(configured.getHost())
                && effectivePort(request) == effectivePort(configured)
                && String.valueOf(request.getScheme()).equalsIgnoreCase(String.valueOf(configured.getScheme()));
    }

    /** The port a URI actually talks to, with the scheme default filled in. */
    private static int effectivePort(URI uri) {
        if (uri.getPort() != -1) {
            return uri.getPort();
        }
        return "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
    }
}
