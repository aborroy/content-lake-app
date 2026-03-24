package org.alfresco.contentlake.batch.service;

import lombok.extern.slf4j.Slf4j;
import org.alfresco.contentlake.batch.config.IngestionProperties;
import org.alfresco.contentlake.batch.model.TransformationTask;
import org.springframework.stereotype.Service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe queue for transformation tasks with basic execution metrics.
 */
@Slf4j
@Service
public class TransformationQueue {

    private final BlockingQueue<TransformationTask> queue;
    private final AtomicInteger pendingCount = new AtomicInteger(0);
    private final AtomicInteger completedCount = new AtomicInteger(0);
    private final AtomicInteger failedCount = new AtomicInteger(0);

    /**
     * Creates a queue with the configured capacity.
     *
     * @param props ingestion configuration
     */
    public TransformationQueue(IngestionProperties props) {
        this.queue = new LinkedBlockingQueue<>(props.getTransform().getQueueCapacity());
    }

    /**
     * Enqueues a transformation task, blocking if the queue is full.
     *
     * @param task task to enqueue
     */
    public void enqueue(TransformationTask task) {
        try {
            queue.put(task);
            pendingCount.incrementAndGet();
            log.debug("Enqueued transformation task for node: {}", task.getNodeId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while enqueueing transformation task", e);
        }
    }

    /**
     * Retrieves and removes a task if available.
     *
     * @return the task, or {@code null} if the queue is empty
     */
    public TransformationTask poll() {
        return queue.poll();
    }

    /**
     * Retrieves and removes a task, blocking until one is available.
     *
     * @return the next task
     * @throws InterruptedException if interrupted while waiting
     */
    public TransformationTask take() throws InterruptedException {
        return queue.take();
    }

    /**
     * Marks a task as successfully completed.
     */
    public void markCompleted() {
        pendingCount.decrementAndGet();
        completedCount.incrementAndGet();
    }

    /**
     * Marks a task as failed.
     */
    public void markFailed() {
        pendingCount.decrementAndGet();
        failedCount.incrementAndGet();
    }

    /**
     * Returns the number of pending tasks.
     */
    public int getPendingCount() {
        return pendingCount.get();
    }

    /**
     * Returns the number of completed tasks.
     */
    public int getCompletedCount() {
        return completedCount.get();
    }

    /**
     * Returns the number of failed tasks.
     */
    public int getFailedCount() {
        return failedCount.get();
    }

    /**
     * Returns the current queue size.
     */
    public int getQueueSize() {
        return queue.size();
    }

    /**
     * Returns whether the queue is empty.
     */
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    /**
     * Clears the queue and resets pending counters.
     */
    public void clear() {
        queue.clear();
        pendingCount.set(0);
    }
}