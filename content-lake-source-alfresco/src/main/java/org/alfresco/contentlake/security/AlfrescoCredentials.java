package org.alfresco.contentlake.security;

/**
 * Holds authentication credentials for Alfresco API calls.
 * Supports both username/password and ticket-based authentication.
 */
public class AlfrescoCredentials {
    private final String username;
    private final String password;
    private final String ticket;
    private final CredentialType type;

    /**
     * Type of authentication credentials.
     */
    public enum CredentialType {
        /** Username and password authentication */
        USERNAME_PASSWORD,
        /** Alfresco ticket authentication */
        TICKET
    }

    /**
     * Creates credentials using username and password.
     *
     * @param username Alfresco username
     * @param password Alfresco password
     */
    public AlfrescoCredentials(String username, String password) {
        this.username = username;
        this.password = password;
        this.ticket = null;
        this.type = CredentialType.USERNAME_PASSWORD;
    }

    /**
     * Creates credentials using an Alfresco ticket.
     *
     * @param ticket Alfresco authentication ticket
     */
    public AlfrescoCredentials(String ticket) {
        this.username = null;
        this.password = null;
        this.ticket = ticket;
        this.type = CredentialType.TICKET;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getTicket() {
        return ticket;
    }

    public CredentialType getType() {
        return type;
    }

    public boolean isTicket() {
        return type == CredentialType.TICKET;
    }
}
