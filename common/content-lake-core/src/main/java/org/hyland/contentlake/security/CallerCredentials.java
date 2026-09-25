package org.hyland.contentlake.security;

import java.util.Map;

/**
 * What a caller presented, in the form a {@link CallerAuthenticator} reads it.
 *
 * <p>A principal and a password cover every credential the service validates today. The
 * {@code attributes} map is what keeps that from being a ceiling: a credential that is neither, such as a
 * bearer token, travels as an attribute rather than as a third {@code PREFIX::} value smuggled through the
 * principal. Two such prefixes already exist and each one is a string every authenticator has to know not
 * to misread.</p>
 *
 * <p>Empty attributes for every Alfresco and Nuxeo authenticator; the map exists for the next identity
 * source rather than for these.</p>
 */
public record CallerCredentials(String principal, String password, Map<String, String> attributes) {

    public CallerCredentials {
        principal = principal == null ? "" : principal;
        password = password == null ? "" : password;
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }

    /** A principal and password with no attributes, which is every credential presented over HTTP Basic. */
    public static CallerCredentials of(String principal, String password) {
        return new CallerCredentials(principal, password, Map.of());
    }

    /** One named attribute, or {@code null} when absent. */
    public String attribute(String name) {
        return attributes.get(name);
    }

    /**
     * Whether no password was presented.
     *
     * <p>Read by the authenticators whose principal carries the whole credential: a ticket or a token is
     * presented as the principal with an empty password, so a non-empty password means the caller meant an
     * ordinary login and the value only looks like a ticket.</p>
     */
    public boolean hasNoPassword() {
        return password.isEmpty();
    }

    /** Whether the principal starts with the given marker. Never throws on a blank principal. */
    public boolean principalStartsWith(String prefix) {
        return principal.startsWith(prefix);
    }
}
