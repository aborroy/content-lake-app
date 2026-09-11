package org.hyland.contentlake.service;

import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.hxpr.api.model.ACE;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.security.AclFilterBuilder;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.contentlake.spi.TextExtractor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The written ACL has to use the principal the read side matches on.
 *
 * <p>Found by the #132 end-to-end run, and not specific to plugin connectors. A source with no ACL model
 * of its own configures its read principal directly, and the natural value is the one that appears in the
 * index: {@code __Everyone__}. That is the already-mapped form, so the write side used to treat it as a
 * username and namespace it to {@code __Everyone___#_<sourceId>} -- which
 * {@link AclFilterBuilder#everyoneClause()} can never match. The document is ingested, embedded, reported
 * as INDEXED, and invisible to every caller. The filesystem connector's documented default read principal
 * is exactly that value.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class NodeSyncServiceEveryonePrincipalTest {

    private static final String SOURCE_ID = "sample-directory";
    private static final String FORMATTED_SOURCE_ID = "sample-directory:sample-directory";

    @Mock
    private ContentSourceClient sourceClient;
    @Mock
    private HxprDocumentApi documentApi;
    @Mock
    private HxprService hxprService;
    @Mock
    private TextExtractor textExtractor;
    @Mock
    private EmbeddingService embeddingService;
    @Mock
    private SimpleChunkingService chunkingService;

    private NodeSyncService service;

    @BeforeEach
    void setUp() {
        service = new NodeSyncService(sourceClient, documentApi, hxprService, textExtractor,
                embeddingService, chunkingService, "/connector-sync", null, false, true);
        when(hxprService.findByNodeId(anyString(), anyString())).thenReturn(null);
        when(hxprService.findByPath(anyString())).thenReturn(null);
        when(hxprService.createDocument(anyString(), any(HxprDocument.class)))
                .thenAnswer(invocation -> {
                    HxprDocument created = invocation.getArgument(1);
                    created.setSysId("hxpr-1");
                    return created;
                });
    }

    /** The already-mapped form must be written verbatim, or the public clause matches nothing. */
    @Test
    void theUnNamespacedEveryonePrincipalIsWrittenVerbatim() {
        service.ingestMetadata(node(Set.of(AclFilterBuilder.EVERYONE_PRINCIPAL)));

        assertThat(userIdsOfWrittenAcl()).containsExactly(AclFilterBuilder.EVERYONE_PRINCIPAL);
    }

    /** The source-system authority keeps mapping to the same principal, as before. */
    @Test
    void theSourceEveryoneAuthorityStillMapsToTheSamePrincipal() {
        service.ingestMetadata(node(Set.of(AclFilterBuilder.EVERYONE_AUTHORITY)));

        assertThat(userIdsOfWrittenAcl()).containsExactly(AclFilterBuilder.EVERYONE_PRINCIPAL);
    }

    /** Everything else is still namespaced: this fix must not widen a real user's principal. */
    @Test
    void anOrdinaryUserIsStillNamespaced() {
        service.ingestMetadata(node(Set.of("alice")));

        assertThat(userIdsOfWrittenAcl()).containsExactly("alice_#_" + SOURCE_ID);
    }

    /** And the public principal written alongside a real one does not disturb it. */
    @Test
    void aMixOfPublicAndUserPrincipalsIsMappedPerPrincipal() {
        service.ingestMetadata(node(Set.of(AclFilterBuilder.EVERYONE_PRINCIPAL, "alice")));

        assertThat(userIdsOfWrittenAcl())
                .containsExactlyInAnyOrder(AclFilterBuilder.EVERYONE_PRINCIPAL, "alice_#_" + SOURCE_ID);
    }

    /**
     * The document is retrievable only if the written principal is what the filter asks for, so the two
     * are asserted against each other rather than against a repeated literal.
     */
    @Test
    void theWrittenPrincipalIsTheOneTheReadSideMatches() {
        service.ingestMetadata(node(Set.of(AclFilterBuilder.EVERYONE_PRINCIPAL)));

        assertThat(AclFilterBuilder.everyoneClause())
                .contains("'" + userIdsOfWrittenAcl().getFirst() + "'");
    }

    private List<String> userIdsOfWrittenAcl() {
        ArgumentCaptor<HxprDocument> captor = ArgumentCaptor.forClass(HxprDocument.class);
        verify(hxprService).createDocument(anyString(), captor.capture());
        return captor.getValue().getSysAcl().stream()
                .map(ACE::getUser)
                .filter(user -> user != null)
                .map(user -> user.getId())
                .toList();
    }

    private static SourceNode node(Set<String> readPrincipals) {
        return new SourceNode(
                "/data/connector/a.txt",
                SOURCE_ID,
                "sample-directory",
                "a.txt",
                "/data/connector",
                "text/plain",
                OffsetDateTime.parse("2026-09-11T08:00:00Z"),
                false,
                readPrincipals,
                Set.of(),
                Map.of("source_nodeId", "/data/connector/a.txt"));
    }
}
