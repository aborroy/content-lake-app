package org.hyland.contentlake.rag.config;

import org.hyland.contentlake.rag.conversation.ConversationMemoryStore;
import org.hyland.contentlake.rag.conversation.InMemoryConversationMemoryStore;
import org.junit.jupiter.api.Test;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class RagAppConfigConversationMemoryStoreTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(RagAppConfig.class)
            .withPropertyValues(
                    "hxpr.url=http://localhost:8080",
                    "hxpr.repository-id=default",
                    "hxpr.username=test-user",
                    "hxpr.password=test-password",
                    // Required: the hxpr embedding type is derived from it, and a blank model is
                    // rejected rather than silently defaulted (#113).
                    "spring.ai.openai.embedding.model=ai/mxbai-embed-large"
            )
            .withBean(EmbeddingModel.class, () -> mock(EmbeddingModel.class))
            // RagProperties is a @Component in the running application, so this slice has to supply it:
            // the multi-embedding-type beans (#121) read rag.embedding.* from it.
            .withBean(RagProperties.class, RagProperties::new);

    @Test
    void conversationMemoryStore_withoutCustomBean_usesInMemoryDefault() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(ConversationMemoryStore.class);
            assertThat(context.getBean(ConversationMemoryStore.class))
                    .isInstanceOf(InMemoryConversationMemoryStore.class);
        });
    }

    @Test
    void conversationMemoryStore_withCustomBean_overridesDefault() {
        ConversationMemoryStore customStore = mock(ConversationMemoryStore.class);
        contextRunner
                .withBean(ConversationMemoryStore.class, () -> customStore)
                .run(context -> {
                    assertThat(context).hasSingleBean(ConversationMemoryStore.class);
                    assertThat(context.getBean(ConversationMemoryStore.class)).isSameAs(customStore);
                });
    }
}
