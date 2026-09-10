package org.hyland.contentlake.service;

import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.Chunk;
import org.hyland.contentlake.model.ChunkType;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.service.chunking.NoiseReductionService;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.service.chunking.strategy.ChunkingStrategy.ChunkingConfig;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.TextExtractor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.Resource;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Text and markdown source documents need no transform engine of any kind.
 *
 * <p>{@code NodeSyncService.extractText} short-circuits on any {@code text/*} MIME type and reads the
 * source bytes directly, so no {@link TextExtractor} is consulted. Since {@code text/markdown} is one
 * of those types, a markdown document reaches chunking with its headings and pipe tables intact and
 * gets the full structure-aware treatment on a deployment that has no transform engine deployed at
 * all. These tests pin that, because it is the difference between the table-aware pipeline being
 * usable by everyone and being usable only with extra infrastructure.</p>
 *
 * <p>A real {@link SimpleChunkingService} is used rather than a mock, so classification and noise
 * reduction actually run.</p>
 */
@ExtendWith(MockitoExtension.class)
class NodeSyncServiceTextAndMarkdownSourceTest {

    private static final String MARKDOWN = """
            # Service Tier Matrix

            This matrix is the authoritative statement of what each subscription tier includes.

            | Tier | Severity 1 response | Coverage |
            | --- | --- | --- |
            | Starter | 8 business hours | 09:00-17:00 weekdays |
            | Growth | 2 business hours | 07:00-19:00 weekdays |
            | Sovereign | 15 minutes | 24x7 |

            Limits are enforced per tenant.
            """;

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

    private NodeSyncService service;

    @BeforeEach
    void setUp() {
        SimpleChunkingService chunkingService = new SimpleChunkingService(
                new NoiseReductionService(false), ChunkingConfig.defaults());
        service = new NodeSyncService(
                sourceClient,
                documentApi,
                hxprService,
                textExtractor,
                embeddingService,
                chunkingService,
                "/filesystem-sync",
                null,
                false,
                true
        );
    }

    @Test
    void markdownSourceYieldsATableChunkWithNoExtractorInvolved() {
        stubIngest();

        service.processContent(
                "hxpr-doc-1",
                Map.of("source_nodeId", "node-1"),
                "node-1",
                "text/markdown",
                "service-tier-matrix.md",
                "/data/service-tier-matrix.md"
        );

        // No extractor is consulted at all: no transform engine, no Tika, no Nuxeo conversion.
        verify(textExtractor, never()).supports(anyString());
        verify(textExtractor, never()).extract(any(Resource.class), anyString());
        verify(textExtractor, never()).extract(anyString(), anyString());
        verify(sourceClient, never()).downloadContent(any(), any());

        List<Chunk> chunks = capturedChunks();
        assertThat(chunks).isNotEmpty();

        List<Chunk> tables = chunks.stream().filter(c -> c.getChunkType() == ChunkType.TABLE).toList();
        assertThat(tables)
                .as("a markdown pipe table must be classified TABLE without any transform engine")
                .hasSize(1);
        assertThat(tables.get(0).getText())
                .contains("| Starter | 8 business hours | 09:00-17:00 weekdays |")
                .contains("| Sovereign | 15 minutes | 24x7 |");
    }

    @Test
    void markdownSourceMirrorsStrippedTextForKeywordMatching() {
        stubIngest();

        service.processContent(
                "hxpr-doc-1",
                Map.of("source_nodeId", "node-1"),
                "node-1",
                "text/markdown",
                "service-tier-matrix.md",
                "/data/service-tier-matrix.md"
        );

        HxprDocument update = capturedUpdate();

        assertThat(update.getSysFulltextBinary())
                .doesNotContain("|")
                .doesNotContain("---")
                .doesNotContain("# ")
                .contains("Service Tier Matrix")
                .contains("Sovereign  15 minutes  24x7");

        assertThat(update.getCinIngestProperties())
                .extractingByKey(ContentLakeIngestProperties.CONTENT_LAKE_EXTRACTED_TEXT)
                .asString()
                .doesNotContain("|")
                .contains("Sovereign  15 minutes  24x7");
    }

    @Test
    void plainTextSourceIsStoredVerbatimAndClassifiedProse() {
        String plain = """
                SERVICE TIER MATRIX

                Tier        Severity 1 response   Coverage
                ----------  --------------------  --------
                Starter     8 business hours      Weekdays
                Sovereign   15 minutes            24x7
                """;
        when(sourceClient.getContent("node-2")).thenReturn(plain.getBytes(StandardCharsets.UTF_8));
        when(embeddingService.embedChunks(any(), any())).thenAnswer(invocation -> {
            List<Chunk> chunks = invocation.getArgument(0);
            return chunks.stream()
                    .map(c -> new EmbeddingService.ChunkWithEmbedding(c, List.of(0.1d)))
                    .toList();
        });
        when(documentApi.updateById(eq("hxpr-doc-2"), any())).thenReturn(new HxprDocument());

        service.processContent(
                "hxpr-doc-2",
                Map.of("source_nodeId", "node-2"),
                "node-2",
                "text/plain",
                "service-tier-matrix.txt",
                "/data/service-tier-matrix.txt"
        );

        // A fixed-width table carries no pipes, so nothing is a table. This is the measured status quo
        // for the plaintext corpus, pinned here so a change to detection shows up as a test change.
        assertThat(capturedChunks()).allSatisfy(
                chunk -> assertThat(chunk.getChunkType()).isEqualTo(ChunkType.PROSE));

        // Plaintext is not markdown, so the stripper is a no-op and the mirror is byte-identical.
        assertThat(capturedUpdate().getSysFulltextBinary()).isEqualTo(plain);
    }

    // ──────────────────────────────────────────────────────────────────────

    private void stubIngest() {
        when(sourceClient.getContent("node-1")).thenReturn(MARKDOWN.getBytes(StandardCharsets.UTF_8));
        when(embeddingService.embedChunks(any(), any())).thenAnswer(invocation -> {
            List<Chunk> chunks = invocation.getArgument(0);
            return chunks.stream()
                    .map(c -> new EmbeddingService.ChunkWithEmbedding(c, List.of(0.1d)))
                    .toList();
        });
        when(documentApi.updateById(eq("hxpr-doc-1"), any())).thenReturn(new HxprDocument());
    }

    @SuppressWarnings("unchecked")
    private List<Chunk> capturedChunks() {
        ArgumentCaptor<List<Chunk>> captor = ArgumentCaptor.forClass(List.class);
        verify(embeddingService).embedChunks(captor.capture(), any());
        return captor.getValue();
    }

    private HxprDocument capturedUpdate() {
        ArgumentCaptor<HxprDocument> captor = ArgumentCaptor.forClass(HxprDocument.class);
        verify(documentApi).updateById(anyString(), captor.capture());
        return captor.getValue();
    }
}
