package ${package};

import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceNode;
import org.springframework.core.io.Resource;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads content and metadata from ${sourceType}.
 *
 * <p>The four methods that matter are {@link #getNode}, {@link #getChildren}, {@link #downloadContent} and
 * {@link #getContent}; the pipeline needs nothing else. Each is stubbed here, so the project compiles and
 * its tests pass before any of the source's API has been touched.</p>
 *
 * <p>Two things to get right when filling them in:</p>
 * <ul>
 *   <li><strong>Read principals.</strong> {@link SourceNode#readPrincipals()} is what the RAG service
 *       filters retrieval by. A node whose permissions cannot be read must not be returned with a
 *       permissive set: fail closed by leaving it out, or the content becomes readable by everyone.</li>
 *   <li><strong>Modification time.</strong> {@link SourceNode#modifiedAt()} is what lets a re-sync skip
 *       unchanged content. Returning {@code null}, or a time that always moves, makes every sync
 *       re-extract and re-embed everything.</li>
 * </ul>
 */
public class SourceConnectorClient implements ContentSourceClient {

    private final String baseUrl;
    private final String username;
    private final String password;
    private final int pageSize;

    public SourceConnectorClient(String baseUrl, String username, String password, int pageSize) {
        this.baseUrl = baseUrl;
        this.username = username;
        this.password = password;
        this.pageSize = pageSize;
    }

    @Override
    public String getSourceId() {
        // Identifies this instance of the source, and becomes the second half of cin_sourceId. A repository
        // identifier is better than a URL when the source has one, because a URL can change.
        return baseUrl;
    }

    @Override
    public String getSourceType() {
        return "${sourceType}";
    }

    @Override
    public SourceNode getNode(String nodeId) {
        // TODO: fetch the node and map it. Return null when it does not exist -- the pipeline treats that
        // as "gone" rather than as an error.
        return new SourceNode(
                nodeId,
                getSourceId(),
                getSourceType(),
                nodeId,
                "/",
                "application/octet-stream",
                null,
                false,
                Set.of(),
                Set.of(),
                Map.of());
    }

    @Override
    public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
        // TODO: list one page of children, honouring skip and maxItems. pageSize is the configured default.
        return List.of();
    }

    @Override
    public Resource downloadContent(String nodeId, String fileName) {
        // TODO: download the binary to a temp file and return it. The caller deletes it.
        return null;
    }

    @Override
    public byte[] getContent(String nodeId) {
        // TODO: return the binary. Used where a temp file would be wasteful.
        return new byte[0];
    }

    int pageSize() {
        return pageSize;
    }

    String username() {
        return username;
    }

    boolean hasCredentials() {
        return username != null && !username.isBlank() && password != null && !password.isBlank();
    }
}
