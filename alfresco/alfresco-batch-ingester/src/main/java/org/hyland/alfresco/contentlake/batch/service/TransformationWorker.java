package org.hyland.alfresco.contentlake.batch.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.alfresco.contentlake.batch.config.IngestionProperties;
import org.hyland.alfresco.contentlake.batch.model.TransformationTask;
import org.hyland.contentlake.service.NodeSyncService;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Background worker that delegates queued content processing to the shared
 * {@link NodeSyncService} pipeline.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TransformationWorker {

    private final TransformationQueue queue;
    private final NodeSyncService nodeSyncService;
    private final IngestionProperties props;

    private ExecutorService executor;
    private volatile boolean running = true;

    @PostConstruct
    public void start() {
        int workerCount = props.getTransform().getWorkerThreads();
        executor = Executors.newFixedThreadPool(workerCount);

        for (int i = 0; i < workerCount; i++) {
            executor.submit(this::processLoop);
        }

        log.info("Started {} transformation workers", workerCount);
    }

    @PreDestroy
    public void stop() {
        running = false;

        if (executor == null) {
            return;
        }

        executor.shutdownNow();
        try {
            boolean terminated = executor.awaitTermination(5, TimeUnit.SECONDS);
            if (!terminated) {
                log.warn("TransformationWorker executor did not terminate within 5 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("TransformationWorker shutdown interrupted", e);
        }
    }

    private void processLoop() {
        while (running) {
            try {
                TransformationTask task = queue.take();
                processTask(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Error in transformation worker", e);
            }
        }
    }

    private void processTask(TransformationTask task) {
        log.debug("Processing transformation for node: {}", task.getNodeId());

        try {
            nodeSyncService.processContent(
                    task.getHxprDocumentId(),
                    task.getIngestProperties(),
                    task.getNodeId(),
                    task.getMimeType(),
                    task.getDocumentName(),
                    task.getDocumentPath()
            );
            queue.markCompleted();
        } catch (Exception e) {
            log.error("Failed transformation for node: {}", task.getNodeId(), e);
            queue.markFailed();
        }
    }
}
