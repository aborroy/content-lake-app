package org.hyland.alfresco.contentlake.batch.service;

import org.hyland.alfresco.contentlake.batch.config.IngestionProperties;
import org.hyland.alfresco.contentlake.batch.model.BatchSyncRequest;
import org.hyland.alfresco.contentlake.batch.model.IngestionJob;
import org.hyland.alfresco.contentlake.client.AlfrescoClient;
import org.hyland.contentlake.service.IndexReconciliationService;
import org.hyland.contentlake.service.NodeSyncService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * A batch job cannot be left {@code RUNNING} for ever by a throwable nobody catches (#153).
 *
 * <p>Two things combined to make the failure silent. {@code catch (Exception)} does not catch an {@code Error}
 * or any other {@code Throwable}, and the task was launched with {@code CompletableFuture.runAsync(...)} whose
 * returned future was <em>discarded</em>, so whatever escaped the catch was captured into that future and never
 * observed, never logged, never surfaced. The job kept {@code status: RUNNING} and {@code completedAt: null}
 * indefinitely.</p>
 *
 * <p>A job marked {@code FAILED} is actionable; a job left {@code RUNNING} is indistinguishable from a slow
 * one, which is why this is worth a test per ingester rather than one shared assertion.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BatchIngestionServiceThrowableTest {

    @Mock
    private NodeDiscoveryService discoveryService;
    @Mock
    private MetadataIngester metadataIngester;
    @Mock
    private TransformationQueue transformationQueue;
    @Mock
    private IndexReconciliationService reconciliationService;
    @Mock
    private NodeSyncService nodeSyncService;
    @Mock
    private AlfrescoClient alfrescoClient;

    private BatchIngestionService service;

    /** Synchronous, so a test can assert on job state once the call returns. */
    private final Executor syncExecutor = Runnable::run;

    @BeforeEach
    void setUp() {
        service = new BatchIngestionService(discoveryService, metadataIngester, transformationQueue,
                syncExecutor, reconciliationService, nodeSyncService, alfrescoClient, new IngestionProperties());
    }

    @Test
    void configuredSyncEndsFailedWhenTheTaskThrowsAnError() {
        when(discoveryService.discoverFromConfigTallied())
                .thenThrow(new StackOverflowError("thrown from the extraction chain"));

        IngestionJob job = service.startConfiguredSync();

        assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.FAILED);
        assertThat(job.getCompletedAt()).isNotNull();
    }

    @Test
    void batchSyncEndsFailedWhenTheTaskThrowsAnError() {
        BatchSyncRequest request = new BatchSyncRequest();
        when(discoveryService.discoverNodesTallied(request))
                .thenThrow(new StackOverflowError("thrown from the extraction chain"));

        IngestionJob job = service.startBatchSync(request);

        assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.FAILED);
        assertThat(job.getCompletedAt()).isNotNull();
    }

    @Test
    void configuredSyncEndsFailedWhenTheTaskThrowsAThrowableThatIsNeitherExceptionNorError() {
        when(discoveryService.discoverFromConfigTallied()).thenAnswer(invocation -> {
            throw new NeitherExceptionNorError();
        });

        IngestionJob job = service.startConfiguredSync();

        assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.FAILED);
        assertThat(job.getCompletedAt()).isNotNull();
    }

    /** Extends Throwable directly, which is the case neither {@code catch} clause used to reach. */
    private static final class NeitherExceptionNorError extends Throwable {
    }
}
