package org.hyland.contentlake.pluginhost.batch.controller;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.pluginhost.batch.config.SelectedConnector;
import org.hyland.contentlake.service.IndexReconciliationService;
import org.hyland.contentlake.service.RootSelection;
import org.hyland.contentlake.service.RootSelectionStore;
import org.hyland.contentlake.spi.SourceNode;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Which roots the loaded connector syncs, readable and writable without a restart.
 *
 * <p>This is the write half of an operator screen. Roots were startup configuration, so changing a scope meant
 * editing a compose file and restarting a container; a pass now asks the selection store instead.</p>
 *
 * <p>Scope only. There is deliberately no way here to add, remove or reconfigure a connector, or to supply a
 * credential: which connector this host runs stays startup configuration, for the reason
 * {@code SyncController} documents. The blast radius of this endpoint is which subtree gets indexed.</p>
 *
 * <p>Authenticated by the host's default-deny rule, like every other endpoint here. Present only when a
 * selection store is configured, because an endpoint that accepted a selection nothing would ever read is
 * worse than a 404.</p>
 */
@Slf4j
@RestController
@RequestMapping("/api/selection")
public class SelectionController {

    private final ObjectProvider<RootSelectionStore> selectionStores;
    private final SelectedConnector connector;

    public SelectionController(ObjectProvider<RootSelectionStore> selectionStores,
                               SelectedConnector connector) {
        this.selectionStores = selectionStores;
        this.connector = connector;
    }

    /** What a pass would walk, and where that came from. */
    @GetMapping
    public ResponseEntity<SelectionView> current() {
        RootSelectionStore store = selectionStores.getIfAvailable();
        if (store == null) {
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build();
        }

        Optional<RootSelection> selection = store.load(qualifiedSourceId());
        return ResponseEntity.ok(new SelectionView(
                connector.sourceType(),
                qualifiedSourceId(),
                selection.map(RootSelection::rootNodeIds).orElse(List.of()),
                selection.isPresent(),
                selection.map(RootSelection::updatedAt).orElse(null),
                selection.map(RootSelection::updatedBy).orElse(null)));
    }

    /**
     * Replaces the selection.
     *
     * <p>Every submitted node id is resolved against the connector before anything is stored. A selection
     * naming a node the source does not have produces a pass that is permanently incomplete, which is the
     * hardest failure to diagnose from outside: the sync succeeds, indexes nothing from that root, and the
     * sweep declines to act on it. Rejecting the whole request here turns that into a 400 an operator can
     * read.</p>
     */
    @PutMapping
    public ResponseEntity<?> replace(@RequestBody SelectionRequest request, Principal principal) {
        RootSelectionStore store = selectionStores.getIfAvailable();
        if (store == null) {
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build();
        }

        List<String> requested = request == null || request.rootNodeIds() == null
                ? List.of()
                : request.rootNodeIds().stream()
                        .filter(id -> id != null && !id.isBlank())
                        .map(String::trim)
                        .distinct()
                        .toList();

        List<String> unknown = new ArrayList<>();
        for (String nodeId : requested) {
            if (!resolves(nodeId)) {
                unknown.add(nodeId);
            }
        }
        if (!unknown.isEmpty()) {
            return ResponseEntity.badRequest().body(new Problem(
                    "These node ids do not resolve in source '" + connector.sourceType()
                            + "', so the selection was not changed: " + unknown,
                    unknown));
        }

        String by = principal == null ? "unknown" : principal.getName();
        store.save(qualifiedSourceId(), RootSelection.of(requested, by));
        log.info("{} set the root selection for {} to {} root(s)", by, qualifiedSourceId(), requested.size());
        return current();
    }

    /** Forgets the selection, so the next pass falls back to configured roots and then to the connector's. */
    @DeleteMapping
    public ResponseEntity<?> clear(Principal principal) {
        RootSelectionStore store = selectionStores.getIfAvailable();
        if (store == null) {
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build();
        }
        store.clear(qualifiedSourceId());
        log.info("{} cleared the root selection for {}",
                principal == null ? "unknown" : principal.getName(), qualifiedSourceId());
        return current();
    }

    /**
     * Whether the connector can resolve a node id.
     *
     * <p>{@code getNode} returns {@code null} for a node that is not there and throws when the source cannot
     * answer. Both mean "do not store this": the first because the root does not exist, the second because
     * nothing here can tell a bad id from an outage, and storing an unverified selection is the outcome this
     * check exists to avoid.</p>
     */
    private boolean resolves(String nodeId) {
        try {
            SourceNode node = connector.client().getNode(nodeId);
            return node != null;
        } catch (RuntimeException e) {
            log.warn("Could not resolve '{}' while validating a root selection: {}", nodeId, e.getMessage());
            return false;
        }
    }

    private String qualifiedSourceId() {
        return IndexReconciliationService.qualifiedSourceId(connector.client());
    }

    /** What to select. A body with an empty list is a valid request meaning "nothing". */
    public record SelectionRequest(List<String> rootNodeIds) {
    }

    /**
     * @param chosen {@code false} when nobody has selected roots, which is not the same as having selected
     *               none: the first falls through to configured roots, the second does not
     */
    public record SelectionView(String sourceType,
                                String qualifiedSourceId,
                                List<String> rootNodeIds,
                                boolean chosen,
                                OffsetDateTime updatedAt,
                                String updatedBy) {
    }

    public record Problem(String message, List<String> unresolved) {
    }
}
