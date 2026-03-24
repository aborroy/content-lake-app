package org.alfresco.contentlake.batch.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * Configuration for the executor that runs batch ingestion jobs.
 *
 * <p>This executor is intentionally isolated from the JVM common pool
 * to provide:
 * <ul>
 *   <li>Predictable concurrency</li>
 *   <li>Bounded queueing and back-pressure</li>
 *   <li>Clean shutdown semantics</li>
 * </ul>
 *
 * <p>It is used explicitly by {@code BatchIngestionService}.
 */
@Configuration
@EnableConfigurationProperties(BatchIngestionExecutorConfig.BatchIngestionExecutorProperties.class)
public class BatchIngestionExecutorConfig {

    /**
     * Executor dedicated to batch ingestion orchestration.
     *
     * <p>Thread naming and shutdown behavior are configured to make
     * ingestion activity easy to identify and terminate cleanly.
     */
    @Bean(name = "batchIngestionExecutor")
    public Executor batchIngestionExecutor(BatchIngestionExecutorProperties props) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(props.getCoreSize());
        executor.setMaxPoolSize(props.getMaxSize());
        executor.setQueueCapacity(props.getQueueCapacity());
        executor.setThreadNamePrefix("batch-ingestion-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(props.getAwaitTerminationSeconds());
        executor.initialize();
        return executor;
    }

    /**
     * Configuration properties for the batch ingestion executor.
     *
     * <p>Bound from {@code ingestion.batch.executor.*} in {@code application.yml}.
     */
    @Data
    @ConfigurationProperties(prefix = "ingestion.batch.executor")
    public static class BatchIngestionExecutorProperties {

        /**
         * Core number of threads kept alive in the pool.
         */
        private int coreSize = 1;

        /**
         * Maximum number of threads allowed in the pool.
         */
        private int maxSize = 1;

        /**
         * Maximum number of queued ingestion tasks before back-pressure applies.
         */
        private int queueCapacity = 1000;

        /**
         * Seconds to wait during shutdown for running tasks to complete.
         */
        private int awaitTerminationSeconds = 30;
    }
}
