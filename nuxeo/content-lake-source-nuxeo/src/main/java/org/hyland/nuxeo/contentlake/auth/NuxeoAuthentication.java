package org.hyland.contentlake.auth;

import org.springframework.http.HttpHeaders;
import org.springframework.http.client.ClientHttpRequestInterceptor;

/**
 * Applies authentication details to outgoing Nuxeo REST requests.
 *
 * <p>The current MVP uses Basic auth, but the client depends on this abstraction
 * so token- or JWT-based auth can be introduced later without rewriting the
 * HTTP layer.</p>
 */
public interface NuxeoAuthentication {

    void apply(HttpHeaders headers);

    default ClientHttpRequestInterceptor asInterceptor() {
        return (request, body, execution) -> {
            apply(request.getHeaders());
            return execution.execute(request, body);
        };
    }
}
