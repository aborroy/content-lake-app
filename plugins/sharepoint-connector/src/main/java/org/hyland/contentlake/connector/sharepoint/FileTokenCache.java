package org.hyland.contentlake.connector.sharepoint;

import com.microsoft.aad.msal4j.ITokenCacheAccessAspect;
import com.microsoft.aad.msal4j.ITokenCacheAccessContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * An msal4j token cache kept in a file, so a delegated sign-in survives a container restart.
 *
 * <p>This is the whole reason the device-code mode is usable at all. Without it a restart means a human in
 * a browser, which is the limitation that made a hand-pasted token unfit for a deployment.</p>
 *
 * <h3>The file is a credential</h3>
 * <p>It holds a refresh token, which lives far longer than the access tokens it mints and can be redeemed
 * from anywhere. It is created with owner-only permissions and its contents are never logged, not even at
 * the finest level. Nothing here ever puts the serialized cache into a message.</p>
 *
 * <h3>Writes are attempted and allowed to fail</h3>
 * <p>The deployment mounts this read-only on purpose: the container only ever needs to read a refresh
 * token, and a read-only mount means a compromised container can neither corrupt the file nor exfiltrate a
 * rotated credential. Entra issues a new refresh token on each redemption, but the previous one stays valid
 * for its own lifetime, so keeping rotations in memory costs nothing until the original expires.</p>
 *
 * <p>So a failed write is an expected state rather than an error, and it is reported once rather than on
 * every token acquisition. Reporting it every time would put a line in the log for every Graph call the
 * connector makes.</p>
 */
final class FileTokenCache implements ITokenCacheAccessAspect {

    private static final Logger log = Logger.getLogger(FileTokenCache.class.getName());

    private final Path file;

    /** So a read-only mount, which is the intended deployment, reports itself once and not per call. */
    private final AtomicBoolean reportedUnwritable = new AtomicBoolean();

    FileTokenCache(Path file) {
        this.file = file;
    }

    @Override
    public void beforeCacheAccess(ITokenCacheAccessContext context) {
        if (!Files.isReadable(file)) {
            return;
        }
        try {
            String serialized = Files.readString(file, StandardCharsets.UTF_8);
            if (!serialized.isBlank()) {
                context.tokenCache().deserialize(serialized);
            }
        } catch (IOException e) {
            // Named, not swallowed: an unreadable cache is a configuration problem, and the caller's next
            // step is a sign-in it cannot know it needs unless this is visible.
            throw new GraphException("Could not read the SharePoint token cache at " + file
                    + "; check " + SharePointConnectorPlugin.TOKEN_CACHE_PATH_SETTING
                    + " and that the file is readable by this process", e);
        } catch (RuntimeException e) {
            throw new GraphException("The SharePoint token cache at " + file + " is not a cache msal4j can "
                    + "read; delete it and sign in again", e);
        }
    }

    @Override
    public void afterCacheAccess(ITokenCacheAccessContext context) {
        if (!context.hasCacheChanged()) {
            return;
        }
        try {
            write(context.tokenCache().serialize());
        } catch (IOException | RuntimeException e) {
            if (reportedUnwritable.compareAndSet(false, true)) {
                log.info("The SharePoint token cache at " + file + " is not writable, so refreshed tokens "
                        + "are kept in memory for the life of this process. That is the expected state for a "
                        + "read-only mount. Sign in again when the stored refresh token finally expires.");
            }
        }
    }

    /**
     * Replaces the file atomically, so a crash mid-write cannot leave a half-serialized cache that then
     * fails to parse and costs an interactive sign-in to recover.
     */
    private void write(String serialized) throws IOException {
        Path directory = file.toAbsolutePath().getParent();
        if (directory != null) {
            Files.createDirectories(directory);
        }
        Path temporary = Files.createTempFile(directory, ".sharepoint-token-cache", ".tmp");
        try {
            Files.writeString(temporary, serialized, StandardCharsets.UTF_8);
            ownerOnly(temporary);
            try {
                Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    /** Best effort: a filesystem without POSIX permissions is not a reason to refuse to store the cache. */
    private static void ownerOnly(Path path) {
        try {
            Files.setPosixFilePermissions(path,
                    java.util.EnumSet.of(java.nio.file.attribute.PosixFilePermission.OWNER_READ,
                            java.nio.file.attribute.PosixFilePermission.OWNER_WRITE));
        } catch (IOException | UnsupportedOperationException e) {
            // Left as the filesystem made it.
        }
    }

    Path file() {
        return file;
    }
}
