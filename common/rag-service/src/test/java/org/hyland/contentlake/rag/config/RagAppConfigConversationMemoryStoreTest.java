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
                    "hxpr.idp.token-url=http://localhost:8081",
                    "hxpr.idp.client-id=test-client",
                    "hxpr.idp.client-secret=test-secret",
                    "hxpr.idp.username=test-user",
                    "hxpr.idp.password=test-password"
            )
            .withBean(EmbeddingModel.class, () -> mock(EmbeddingModel.class));

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
