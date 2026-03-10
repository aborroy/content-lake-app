package org.alfresco.contentlake.rag.conversation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * One conversational turn in a session.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConversationTurn {

    public enum Role {
        USER,
        ASSISTANT
    }

    private Role role;
    private String content;
    private Instant timestamp;
}
