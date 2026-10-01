package org.hyland.contentlake.rag.conversation;

import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.rag.config.RagProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.Prompt;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SessionSummaryServiceTest {

    @Mock ChatModel chatModel;
    @Mock HxprService hxprService;
    @Mock HxprDocumentApi documentApi;

    private RagProperties properties;
    private SessionSummaryService service;

    @BeforeEach
    void setUp() {
        properties = new RagProperties();
        service = new SessionSummaryService(chatModel, hxprService, documentApi, properties);
    }

    private void enable() {
        properties.getConversation().getSummary().setEnabled(true);
    }

    private static List<ConversationTurn> turns() {
        return List.of(
                ConversationTurn.builder().role(ConversationTurn.Role.USER)
                        .content("What is the retention policy?").timestamp(Instant.now()).build(),
                ConversationTurn.builder().role(ConversationTurn.Role.ASSISTANT)
                        .content("Records are kept for seven years.").timestamp(Instant.now()).build());
    }

    private static ChatResponse chatResponse(String text) {
        return new ChatResponse(List.of(new Generation(new AssistantMessage(text))));
    }

    @Test
    void disabled_isNoOp() {
        assertThat(service.isEnabled()).isFalse();
        assertThat(service.loadSummary("user:alice")).isNull();
        service.updateAfterTurn("user:alice", turns());

        verifyNoInteractions(chatModel, hxprService, documentApi);
    }

    @Test
    void refreshSummary_createsSummaryDocumentWhenNoneExists() {
        enable();
        when(chatModel.call(any(Prompt.class))).thenReturn(chatResponse("User asked about retention; kept 7 years."));
        when(hxprService.findByPath(any())).thenReturn(null);

        service.refreshSummary("user:alice", turns());

        verify(hxprService).ensureFolder("/_sessions");
        ArgumentCaptor<HxprDocument> captor = ArgumentCaptor.forClass(HxprDocument.class);
        verify(hxprService).createDocument(eq("/_sessions"), captor.capture());
        HxprDocument saved = captor.getValue();
        assertThat(saved.getSysName()).isEqualTo(sha256("user:alice"));
        // Without the mixin hxpr rejects the cin_* fields with 400 "cin_ingestPropertyNames".
        assertThat(saved.getSysMixinTypes()).containsExactly(HxprDocument.MIXIN_CIN_REMOTE);
        assertThat((String) saved.getCinIngestProperties().get(SessionSummaryService.SUMMARY_PROPERTY))
                .contains("retention");
        assertThat(saved.getCinIngestPropertyNames())
                .containsExactlyInAnyOrderElementsOf(saved.getCinIngestProperties().keySet());
        verify(documentApi, never()).updateById(any(), any());
    }

    @Test
    void refreshSummary_updatesExistingSummaryDocument() {
        enable();
        HxprDocument existing = new HxprDocument();
        existing.setSysId("sess-doc-1");
        when(chatModel.call(any(Prompt.class))).thenReturn(chatResponse("updated notes"));
        when(hxprService.findByPath(any())).thenReturn(existing);

        service.refreshSummary("user:alice", turns());

        verify(documentApi).updateById(eq("sess-doc-1"), any(HxprDocument.class));
        verify(hxprService, never()).createDocument(any(), any());
    }

    @Test
    void updateAfterTurn_persistsOffTheCallingThread() throws Exception {
        enable();
        CountDownLatch persisted = new CountDownLatch(1);
        when(chatModel.call(any(Prompt.class))).thenReturn(chatResponse("notes"));
        when(hxprService.findByPath(any())).thenReturn(null);
        doAnswer(invocation -> {
            persisted.countDown();
            return null;
        }).when(hxprService).createDocument(any(), any());

        service.updateAfterTurn("user:alice", turns());

        assertThat(persisted.await(5, TimeUnit.SECONDS)).isTrue();
        verify(hxprService).createDocument(eq("/_sessions"), any(HxprDocument.class));
    }

    @Test
    void updateAfterTurn_doesNotBlockOnASlowRefresh() throws Exception {
        enable();
        CountDownLatch refreshStarted = new CountDownLatch(1);
        CountDownLatch releaseRefresh = new CountDownLatch(1);
        CountDownLatch refreshFinished = new CountDownLatch(1);
        when(chatModel.call(any(Prompt.class))).thenAnswer(invocation -> {
            refreshStarted.countDown();
            releaseRefresh.await(5, TimeUnit.SECONDS);
            return chatResponse("notes");
        });
        doAnswer(invocation -> {
            refreshFinished.countDown();
            return null;
        }).when(hxprService).createDocument(any(), any());

        long before = System.nanoTime();
        service.updateAfterTurn("user:alice", turns());
        long elapsedMs = (System.nanoTime() - before) / 1_000_000;

        // The caller -- a streaming response waiting to emit its sources -- returns while the
        // refresh is still inside the LLM call.
        assertThat(refreshStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(elapsedMs).isLessThan(1_000);

        releaseRefresh.countDown();
        assertThat(refreshFinished.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void loadSummary_readsPersistedProperty() {
        enable();
        HxprDocument doc = new HxprDocument();
        doc.setCinIngestProperties(Map.of(SessionSummaryService.SUMMARY_PROPERTY, "prior summary"));
        when(hxprService.findByPath("/_sessions/" + sha256("user:alice"))).thenReturn(doc);

        assertThat(service.loadSummary("user:alice")).isEqualTo("prior summary");
    }

    @Test
    void loadSummary_returnsNullWhenReadFails() {
        enable();
        when(hxprService.findByPath(any())).thenThrow(new RuntimeException("hxpr down"));

        assertThat(service.loadSummary("user:alice")).isNull();
    }

    @Test
    void loadSummary_keysThatCharacterSubstitutionWouldMerge_readDifferentDocuments() {
        enable();
        HxprDocument doc = new HxprDocument();
        doc.setCinIngestProperties(Map.of(SessionSummaryService.SUMMARY_PROPERTY, "alice's summary"));
        lenient().when(hxprService.findByPath(any())).thenReturn(null);
        when(hxprService.findByPath("/_sessions/" + sha256("user:alice"))).thenReturn(doc);

        assertThat(service.loadSummary("user:alice")).isEqualTo("alice's summary");
        assertThat(service.loadSummary("user_alice")).isNull();
    }

    private static String sha256(String value) {
        try {
            return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
