package org.hyland.contentlake.rag.conversation;

/**
 * Derives the key a conversation is stored under from its owner and the client's session id.
 *
 * <p>A session id is chosen by the client, so on its own it proves nothing about who is asking. Every
 * read and write of conversation memory and of the running summary goes through this key instead, so
 * a caller who presents another user's session id reaches an empty conversation of their own rather
 * than that user's history, and cannot reset it either.</p>
 */
public final class ConversationKeys {

    private ConversationKeys() {
    }

    /**
     * @param owner     the authenticated username, never blank
     * @param sessionId the client's session id, never blank
     * @return a key no other {@code (owner, sessionId)} pair produces. The owner is length-prefixed, so
     *         a username or session id containing the separator cannot make two pairs collide
     */
    public static String of(String owner, String sessionId) {
        if (owner == null || owner.isBlank()) {
            throw new IllegalArgumentException("owner is required");
        }
        if (sessionId == null || sessionId.isBlank()) {
            throw new IllegalArgumentException("sessionId is required");
        }
        String o = owner.trim();
        return o.length() + ":" + o + "/" + sessionId.trim();
    }
}
