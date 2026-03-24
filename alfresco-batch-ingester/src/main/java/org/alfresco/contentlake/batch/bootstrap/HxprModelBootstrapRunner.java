package org.alfresco.contentlake.batch.bootstrap;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.alfresco.contentlake.client.HxprModelProvisioner;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Runs HXPR model provisioning once at application startup.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HxprModelBootstrapRunner implements ApplicationRunner {

    private final HxprModelProvisioner modelProvisioner;

    @Value("${hxpr.model.bootstrap.enabled:true}")
    private boolean enabled;

    @Value("${hxpr.model.fragments:classpath:model-fragments.json}")
    private String fragmentsLocation;

    @Override
    public void run(ApplicationArguments args) {
        if (!enabled) {
            log.info("HXPR model bootstrap disabled.");
            return;
        }
        modelProvisioner.ensureModelPresent(fragmentsLocation);
    }
}
