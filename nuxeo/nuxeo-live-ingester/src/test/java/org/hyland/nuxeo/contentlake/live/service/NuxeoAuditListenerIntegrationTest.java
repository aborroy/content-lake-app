package org.hyland.nuxeo.contentlake.live.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.hyland.nuxeo.contentlake.auth.BasicNuxeoAuthentication;
import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprQueryApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.nuxeo.contentlake.client.NuxeoClient;
import org.hyland.nuxeo.contentlake.client.NuxeoConversionClient;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.nuxeo.contentlake.model.NuxeoDocument;
import org.hyland.nuxeo.contentlake.live.client.NuxeoAuditClient;
import org.hyland.nuxeo.contentlake.live.config.NuxeoLiveProperties;
import org.hyland.nuxeo.contentlake.live.model.AuditCursor;
import org.hyland.nuxeo.contentlake.live.model.NuxeoAuditPage;
import org.hyland.contentlake.service.EmbeddingService;
import org.hyland.nuxeo.contentlake.service.NuxeoScopeResolver;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.service.chunking.NoiseReductionService;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.service.chunking.strategy.ChunkingStrategy.ChunkingConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class NuxeoAuditListenerIntegrationTest {

    private static final String NUXEO_BASE_URL = "http://localhost:8081/nuxeo";
    private static final String NUXEO_USERNAME = "Administrator";
    private static final String NUXEO_PASSWORD = "Administrator";
    private static final String SOURCE_ID = "local";
    private static final String SOURCE_TYPE = "nuxeo";
    private static final String REPOSITORY_KEY = SOURCE_TYPE + ":" + SOURCE_ID;
    private static final String WORKSPACES_PATH = "/default-domain/workspaces";
    private static final Pattern NODE_ID_PATTERN = Pattern.compile("cin_id = '((?:''|[^'])*)'");
    private static final Pattern SOURCE_ID_PATTERN = Pattern.compile("cin_sourceId = '((?:''|[^'])*)'");

    @TempDir
    Path tempDir;

    @Test
    void listen_syncsCreateRestoresCursorAfterRestartAndDeletesTrashedDocument() throws Exception {
        assumeTrue(waitForNuxeo(Duration.ofSeconds(90)),
                "Start nuxeo-deployment/compose.yaml before running this integration test");

        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        String namespace = "codex-live-it-" + UUID.randomUUID().toString().substring(0, 8);
        Path cursorFile = tempDir.resolve("audit-cursor.json");
        NuxeoDocument workspace = null;
        NuxeoDocument note = null;

        try (FakeHxprServer hxpr = new FakeHxprServer(objectMapper)) {
            workspace = createWorkspace(namespace);

            ListenerHarness firstHarness = createHarness(hxpr.baseUrl(), cursorFile, workspace.getPath(), objectMapper);
            OffsetDateTime cursorStart = OffsetDateTime.now(ZoneOffset.UTC);
            firstHarness.cursorStore().save(REPOSITORY_KEY, AuditCursor.initial(cursorStart));

            note = createNote(workspace.getPath(), namespace);
            waitForAuditEvent(note.getUid(), "documentCreated", Duration.ofSeconds(30));

            firstHarness.listener().listen();

            HxprDocument storedAfterCreate = hxpr.findFileByNodeId(note.getUid());
            assertThat(storedAfterCreate).isNotNull();
            assertThat(storedAfterCreate.getCinId()).isEqualTo(note.getUid());
            assertThat(storedAfterCreate.getCinSourceId()).isEqualTo(REPOSITORY_KEY);
            assertThat(hxpr.fileDocumentCount()).isEqualTo(1);

            Optional<AuditCursor> savedCursor = firstHarness.cursorStore().load(REPOSITORY_KEY);
            assertThat(savedCursor).isPresent();
            int mutationCountAfterCreate = hxpr.fileMutationCount();
            int createCountAfterCreate = hxpr.fileCreateCount();
            int updateCountAfterCreate = hxpr.fileUpdateCount();

            ListenerHarness restartedHarness = createHarness(hxpr.baseUrl(), cursorFile, workspace.getPath(), objectMapper);
            assertThat(restartedHarness.cursorStore().load(REPOSITORY_KEY)).contains(savedCursor.get());

            restartedHarness.listener().listen();

            assertThat(restartedHarness.cursorStore().load(REPOSITORY_KEY)).contains(savedCursor.get());
            assertThat(hxpr.fileMutationCount()).isEqualTo(mutationCountAfterCreate);
            assertThat(hxpr.fileCreateCount()).isEqualTo(createCountAfterCreate);
            assertThat(hxpr.fileUpdateCount()).isEqualTo(updateCountAfterCreate);
            assertThat(hxpr.fileDocumentCount()).isEqualTo(1);

            trashDocument(note.getUid());
            waitForAuditEvent(note.getUid(), "documentTrashed", Duration.ofSeconds(30));

            restartedHarness.listener().listen();

            assertThat(hxpr.findFileByNodeId(note.getUid())).isNull();
            assertThat(hxpr.fileDocumentCount()).isZero();
            assertThat(hxpr.fileDeleteCount()).isEqualTo(1);
        } finally {
            if (note != null) {
                trashDocumentQuietly(note.getUid());
            }
            if (workspace != null) {
                trashDocumentQuietly(workspace.getUid());
            }
        }
    }

    private static ListenerHarness createHarness(String hxprBaseUrl,
                                                 Path cursorFile,
                                                 String includedRoot,
                                                 ObjectMapper objectMapper) {
        BasicNuxeoAuthentication authentication = new BasicNuxeoAuthentication(NUXEO_USERNAME, NUXEO_PASSWORD);
        Clock clock = Clock.systemUTC();

        NuxeoAuditClient auditClient = new NuxeoAuditClient(NUXEO_BASE_URL, authentication);
        FileAuditCursorStore cursorStore = new FileAuditCursorStore(cursorFile, objectMapper);
        NuxeoClient nuxeoClient = new NuxeoClient(NUXEO_BASE_URL, SOURCE_ID, "file:content", authentication);
        NuxeoScopeResolver scopeResolver = new NuxeoScopeResolver(
                List.of(includedRoot),
                Set.of("Note"),
                Set.of("deleted"),
                nuxeoClient
        );

        RestClient hxprRestClient = RestClient.builder()
                .baseUrl(hxprBaseUrl)
                .build();
        HttpServiceProxyFactory proxyFactory =
                HttpServiceProxyFactory.builderFor(RestClientAdapter.create(hxprRestClient)).build();
        HxprDocumentApi documentApi = proxyFactory.createClient(HxprDocumentApi.class);
        HxprQueryApi queryApi = proxyFactory.createClient(HxprQueryApi.class);
        HxprService hxprService = new HxprService(documentApi, queryApi, hxprRestClient);

        NuxeoConversionClient conversionClient =
                new NuxeoConversionClient(NUXEO_BASE_URL, "file:content", 30_000L, true, authentication);
        EmbeddingService embeddingService =
                new EmbeddingService(Mockito.mock(EmbeddingModel.class), "test-embedding-model");
        SimpleChunkingService chunkingService = new SimpleChunkingService(
                new NoiseReductionService(false, false),
                new ChunkingConfig(200, 1000, 120, 0.75)
        );
        NodeSyncService nodeSyncService = new NodeSyncService(
                nuxeoClient,
                documentApi,
                hxprService,
                conversionClient,
                embeddingService,
                chunkingService,
                "/nuxeo-sync",
                null
        );

        NuxeoLiveProperties props = new NuxeoLiveProperties();
        props.getAudit().setEnabled(true);
        props.getAudit().setInitialLookback(Duration.ofSeconds(5));
        props.getAudit().setPageSize(25);

        NuxeoAuditMetrics metrics = new NuxeoAuditMetrics(new SimpleMeterRegistry(), clock);
        NuxeoAuditListener listener = new NuxeoAuditListener(
                auditClient,
                cursorStore,
                nodeSyncService,
                nuxeoClient,
                scopeResolver,
                props,
                metrics,
                clock
        );
        return new ListenerHarness(listener, cursorStore);
    }

    private static boolean waitForNuxeo(Duration timeout) {
        RestClient client = nuxeoApiClient();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                client.get()
                        .uri("/api/v1/path/default-domain/workspaces")
                        .retrieve()
                        .toBodilessEntity();
                return true;
            } catch (RestClientException ignored) {
                sleep(Duration.ofSeconds(1));
            }
        }
        return false;
    }

    private static NuxeoDocument createWorkspace(String namespace) {
        return nuxeoSiteClient().post()
                .uri("/site/automation/Document.Create")
                .body(Map.of(
                        "params", Map.of(
                                "type", "Workspace",
                                "name", namespace,
                                "properties", "dc:title=" + namespace
                        ),
                        "input", "doc:" + WORKSPACES_PATH
                ))
                .retrieve()
                .body(NuxeoDocument.class);
    }

    private static NuxeoDocument createNote(String workspacePath, String namespace) {
        return nuxeoSiteClient().post()
                .uri("/site/automation/Document.Create")
                .body(Map.of(
                        "params", Map.of(
                                "type", "Note",
                                "name", namespace + "-note",
                                "properties", "dc:title=" + namespace + "-note\nnote:note=integration body"
                        ),
                        "input", "doc:" + workspacePath
                ))
                .retrieve()
                .body(NuxeoDocument.class);
    }

    private static void trashDocument(String uid) {
        nuxeoSiteClient().post()
                .uri("/site/automation/Document.Trash")
                .body(Map.of("input", "doc:" + uid))
                .retrieve()
                .toBodilessEntity();
    }

    private static void trashDocumentQuietly(String uid) {
        try {
            trashDocument(uid);
        } catch (Exception ignored) {
        }
    }

    private static void waitForAuditEvent(String uid, String eventId, Duration timeout) {
        RestClient client = nuxeoApiClient();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            NuxeoAuditPage page = client.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/api/v1/id/{uid}/@audit")
                            .queryParam("pageSize", 20)
                            .build(uid))
                    .retrieve()
                    .body(NuxeoAuditPage.class);
            if (page != null && page.getEntries().stream().anyMatch(entry -> eventId.equals(entry.eventId()))) {
                return;
            }
            sleep(Duration.ofMillis(500));
        }
        throw new AssertionError("Timed out waiting for audit event " + eventId + " on " + uid);
    }

    private static RestClient nuxeoSiteClient() {
        return RestClient.builder()
                .baseUrl(NUXEO_BASE_URL)
                .requestInterceptor(new BasicNuxeoAuthentication(NUXEO_USERNAME, NUXEO_PASSWORD).asInterceptor())
                .build();
    }

    private static RestClient nuxeoApiClient() {
        return RestClient.builder()
                .baseUrl(NUXEO_BASE_URL)
                .requestInterceptor(new BasicNuxeoAuthentication(NUXEO_USERNAME, NUXEO_PASSWORD).asInterceptor())
                .build();
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for Nuxeo audit event", e);
        }
    }

    private record ListenerHarness(
            NuxeoAuditListener listener,
            FileAuditCursorStore cursorStore
    ) {
    }

    private static final class FakeHxprServer implements AutoCloseable, HttpHandler {

        private final ObjectMapper objectMapper;
        private final HttpServer server;
        private final Map<String, HxprDocument> fileDocumentsById = new LinkedHashMap<>();
        private final Map<String, String> fileDocumentIdsByPath = new LinkedHashMap<>();
        private final Set<String> folderPaths = new LinkedHashSet<>();
        private final AtomicInteger nextId = new AtomicInteger(1);
        private final AtomicInteger fileCreateCount = new AtomicInteger();
        private final AtomicInteger fileUpdateCount = new AtomicInteger();
        private final AtomicInteger fileDeleteCount = new AtomicInteger();

        private FakeHxprServer(ObjectMapper objectMapper) throws IOException {
            this.objectMapper = objectMapper;
            this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            this.server.createContext("/api/query", this);
            this.server.createContext("/api/documents", this);
            this.server.start();
        }

        private String baseUrl() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        private synchronized HxprDocument findFileByNodeId(String nodeId) {
            return fileDocumentsById.values().stream()
                    .filter(document -> nodeId.equals(document.getCinId()))
                    .findFirst()
                    .map(this::copy)
                    .orElse(null);
        }

        private int fileDocumentCount() {
            return fileDocumentsById.size();
        }

        private int fileMutationCount() {
            return fileCreateCount.get() + fileUpdateCount.get() + fileDeleteCount.get();
        }

        private int fileCreateCount() {
            return fileCreateCount.get();
        }

        private int fileUpdateCount() {
            return fileUpdateCount.get();
        }

        private int fileDeleteCount() {
            return fileDeleteCount.get();
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                String path = exchange.getRequestURI().getPath();
                if ("/api/query".equals(path)) {
                    handleQuery(exchange);
                    return;
                }
                if (path.startsWith("/api/documents/path/")) {
                    handlePathRequest(exchange);
                    return;
                }
                if (path.startsWith("/api/documents/")) {
                    handleDocumentById(exchange);
                    return;
                }
                sendStatus(exchange, 404);
            } catch (Exception e) {
                sendJson(exchange, 500, Map.of("message", e.getMessage()));
            } finally {
                exchange.close();
            }
        }

        private synchronized void handleQuery(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                sendStatus(exchange, 405);
                return;
            }

            JsonNode request = readBody(exchange, JsonNode.class);
            String query = request.path("query").asText("");
            Set<String> nodeIds = extractValues(query, NODE_ID_PATTERN);
            Set<String> sourceIds = extractValues(query, SOURCE_ID_PATTERN);

            List<HxprDocument> documents = fileDocumentsById.values().stream()
                    .filter(document -> nodeIds.isEmpty() || nodeIds.contains(document.getCinId()))
                    .filter(document -> sourceIds.isEmpty() || sourceIds.contains(document.getCinSourceId()))
                    .map(this::copy)
                    .toList();

            HxprDocument.QueryResult result = new HxprDocument.QueryResult();
            result.setDocuments(documents);
            result.setCount(documents.size());
            result.setTotalCount(documents.size());
            result.setOffset(0);
            result.setLimit(documents.size());
            sendJson(exchange, 200, result);
        }

        private synchronized void handlePathRequest(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            String parentPath = decodeAbsolutePath(exchange.getRequestURI().getRawPath()
                    .substring("/api/documents/path/".length()));

            if ("GET".equals(method)) {
                String documentId = fileDocumentIdsByPath.get(parentPath);
                if (documentId == null) {
                    sendStatus(exchange, 404);
                    return;
                }
                sendJson(exchange, 200, fileDocumentsById.get(documentId));
                return;
            }

            if (!"POST".equals(method)) {
                sendStatus(exchange, 405);
                return;
            }

            HxprDocument request = readBody(exchange, HxprDocument.class);
            if ("SysFolder".equals(request.getSysPrimaryType())) {
                String folderPath = joinPath(parentPath, request.getSysName());
                if (!folderPaths.add(folderPath)) {
                    sendStatus(exchange, 409);
                    return;
                }
                HxprDocument folder = copy(request);
                folder.setSysId("folder-" + nextId.getAndIncrement());
                folder.setSysParentPath(parentPath);
                sendJson(exchange, 201, folder);
                return;
            }

            String documentPath = joinPath(parentPath, request.getSysName());
            if (fileDocumentIdsByPath.containsKey(documentPath)) {
                sendStatus(exchange, 409);
                return;
            }

            HxprDocument created = copy(request);
            created.setSysId("doc-" + nextId.getAndIncrement());
            created.setSysParentPath(parentPath);
            if (created.getCinPaths() == null || created.getCinPaths().isEmpty()) {
                created.setCinPaths(List.of(documentPath));
            }

            fileDocumentsById.put(created.getSysId(), created);
            fileDocumentIdsByPath.put(documentPath, created.getSysId());
            fileCreateCount.incrementAndGet();
            sendJson(exchange, 201, created);
        }

        private synchronized void handleDocumentById(HttpExchange exchange) throws IOException {
            String documentId = exchange.getRequestURI().getPath().substring("/api/documents/".length());
            HxprDocument existing = fileDocumentsById.get(documentId);
            if (existing == null) {
                sendStatus(exchange, 404);
                return;
            }

            switch (exchange.getRequestMethod()) {
                case "GET" -> sendJson(exchange, 200, existing);
                case "PUT" -> {
                    HxprDocument update = readBody(exchange, HxprDocument.class);
                    merge(existing, update);
                    fileUpdateCount.incrementAndGet();
                    sendJson(exchange, 200, existing);
                }
                case "DELETE" -> {
                    fileDocumentsById.remove(documentId);
                    removePathMapping(documentId);
                    fileDeleteCount.incrementAndGet();
                    sendStatus(exchange, 204);
                }
                default -> sendStatus(exchange, 405);
            }
        }

        private void merge(HxprDocument target, HxprDocument update) {
            if (update.getSysPrimaryType() != null) {
                target.setSysPrimaryType(update.getSysPrimaryType());
            }
            if (update.getSysName() != null) {
                target.setSysName(update.getSysName());
            }
            if (update.getSysParentPath() != null) {
                target.setSysParentPath(update.getSysParentPath());
            }
            if (update.getSysMixinTypes() != null) {
                target.setSysMixinTypes(new ArrayList<>(update.getSysMixinTypes()));
            }
            if (update.getSysFulltextBinary() != null) {
                target.setSysFulltextBinary(update.getSysFulltextBinary());
            }
            if (update.getSysAcl() != null) {
                target.setSysAcl(new ArrayList<>(update.getSysAcl()));
            }
            if (update.getCinId() != null) {
                target.setCinId(update.getCinId());
            }
            if (update.getCinSourceId() != null) {
                target.setCinSourceId(update.getCinSourceId());
            }
            if (update.getCinPaths() != null) {
                target.setCinPaths(new ArrayList<>(update.getCinPaths()));
            }
            if (update.getCinIngestProperties() != null) {
                target.setCinIngestProperties(new LinkedHashMap<>(update.getCinIngestProperties()));
            }
            if (update.getCinIngestPropertyNames() != null) {
                target.setCinIngestPropertyNames(new ArrayList<>(update.getCinIngestPropertyNames()));
            }
            if (update.getCinRead() != null) {
                target.setCinRead(new ArrayList<>(update.getCinRead()));
            }
            if (update.getCinDeny() != null) {
                target.setCinDeny(new ArrayList<>(update.getCinDeny()));
            }
            if (update.getSysembedEmbeddings() != null) {
                target.setSysembedEmbeddings(new ArrayList<>(update.getSysembedEmbeddings()));
            }
        }

        private void removePathMapping(String documentId) {
            String matchedPath = fileDocumentIdsByPath.entrySet().stream()
                    .filter(entry -> documentId.equals(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(null);
            if (matchedPath != null) {
                fileDocumentIdsByPath.remove(matchedPath);
            }
        }

        private Set<String> extractValues(String query, Pattern pattern) {
            Set<String> values = new LinkedHashSet<>();
            Matcher matcher = pattern.matcher(query);
            while (matcher.find()) {
                values.add(unescapeHxql(matcher.group(1)));
            }
            return values;
        }

        private String decodeAbsolutePath(String encodedPath) {
            if (encodedPath == null || encodedPath.isBlank()) {
                return "/";
            }
            String decoded = URLDecoder.decode(encodedPath, StandardCharsets.UTF_8);
            return decoded.startsWith("/") ? decoded : "/" + decoded;
        }

        private String joinPath(String parentPath, String name) {
            if (parentPath == null || parentPath.isBlank() || "/".equals(parentPath)) {
                return "/" + name;
            }
            return parentPath + "/" + name;
        }

        private String unescapeHxql(String value) {
            return value.replace("''", "'");
        }

        private <T> T readBody(HttpExchange exchange, Class<T> type) throws IOException {
            try (InputStream inputStream = exchange.getRequestBody()) {
                return objectMapper.readValue(inputStream, type);
            }
        }

        private void sendJson(HttpExchange exchange, int status, Object body) throws IOException {
            byte[] bytes = objectMapper.writeValueAsBytes(body);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(bytes);
            }
        }

        private void sendStatus(HttpExchange exchange, int status) throws IOException {
            exchange.sendResponseHeaders(status, -1);
        }

        private HxprDocument copy(HxprDocument document) {
            return objectMapper.convertValue(document, HxprDocument.class);
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
