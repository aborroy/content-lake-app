package org.hyland.contentlake.rag.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.rag.config.RagProperties;
import org.hyland.contentlake.security.SecurityContextService;
import org.hyland.contentlake.service.EmbeddingBackfillService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.function.Supplier;

/**
 * Embedding backfill endpoints (#121). Present only when
 * {@code rag.embedding.backfill.enabled=true}, which is not the default.
 *
 * <ul>
 *   <li>{@code POST /api/admin/embedding-backfill/start} - begin re-embedding the corpus into the
 *       configured embedding type, optionally overriding the rate.</li>
 *   <li>{@code POST /api/admin/embedding-backfill/pause} - stop after the current document.</li>
 *   <li>{@code POST /api/admin/embedding-backfill/resume} - continue a paused run.</li>
 *   <li>{@code GET /api/admin/embedding-backfill/status} - progress, readable by any authenticated
 *       caller.</li>
 * </ul>
 *
 * <p>The three state-changing operations require an account named in
 * {@code rag.embedding.backfill.operator-users}, which is empty by default, so they are closed until a
 * deployment names an operator. Being merely authenticated is not enough: the job writes to every
 * document in the corpus and spends the embedding throughput the ingesters need. All four sit behind
 * the existing authentication chain, so none is anonymous.</p>
 */
@Slf4j
@RestController
@RequestMapping("/api/admin/embedding-backfill")
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "rag.embedding.backfill", name = "enabled", havingValue = "true")
public class EmbeddingBackfillController {

    private final EmbeddingBackfillService backfillService;
    private final SecurityContextService securityContextService;
    private final RagProperties ragProperties;

    @PostMapping("/start")
    public ResponseEntity<EmbeddingBackfillService.Progress> start(
            @RequestParam(value = "docsPerMinute", required = false) Integer docsPerMinute) {
        String operator = requireOperator("start");
        int rate = docsPerMinute != null
                ? docsPerMinute
                : ragProperties.getEmbedding().getBackfill().getDocsPerMinute();
        log.info("Operator {} started an embedding backfill into '{}' at {} docs/minute",
                operator, backfillService.targetType(), rate);
        return run(backfillService::status, () -> backfillService.start(rate));
    }

    @PostMapping("/pause")
    public ResponseEntity<EmbeddingBackfillService.Progress> pause() {
        String operator = requireOperator("pause");
        log.info("Operator {} paused the embedding backfill", operator);
        return ResponseEntity.ok(backfillService.pause());
    }

    @PostMapping("/resume")
    public ResponseEntity<EmbeddingBackfillService.Progress> resume() {
        String operator = requireOperator("resume");
        log.info("Operator {} resumed the embedding backfill", operator);
        return run(backfillService::status, backfillService::resume);
    }

    @GetMapping("/status")
    public ResponseEntity<EmbeddingBackfillService.Progress> status() {
        return ResponseEntity.ok(backfillService.status());
    }

    /**
     * Runs a state change, answering 409 rather than 500 when the job is not in a state that allows it.
     *
     * <p>Starting a run that is already going, or resuming one that is not paused, is a conflict with
     * the current state and an operator needs to see it as one. The body carries the progress, so the
     * response says what state the job is actually in.</p>
     */
    private ResponseEntity<EmbeddingBackfillService.Progress> run(
            Supplier<EmbeddingBackfillService.Progress> onConflict,
            Supplier<EmbeddingBackfillService.Progress> action) {
        try {
            return ResponseEntity.ok(action.get());
        } catch (IllegalStateException e) {
            log.info("Embedding backfill state change rejected: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.CONFLICT).body(onConflict.get());
        }
    }

    private String requireOperator(String action) {
        String username = securityContextService.getCurrentUsername();
        if (!isOperator(username)) {
            log.warn("Refusing to {} the embedding backfill for {}: not a configured operator",
                    action, username);
            throw new AccessDeniedException(
                    "Controlling the embedding backfill requires an operator account");
        }
        return username;
    }

    private boolean isOperator(String username) {
        List<String> operators = ragProperties.getEmbedding().getBackfill().getOperatorUsers();
        if (operators == null || operators.isEmpty()) {
            return false;
        }
        // Case-insensitive, because Alfresco authenticates usernames case-insensitively, so "Admin"
        // and "admin" are the same account and an operator list that distinguished them would surprise.
        return operators.stream()
                .filter(operator -> operator != null && !operator.isBlank())
                .anyMatch(operator -> operator.trim().equalsIgnoreCase(username));
    }
}
