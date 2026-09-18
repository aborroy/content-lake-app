package org.hyland.contentlake.pluginhost.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Batch ingester driven by a connector loaded from the plugin directory (#132).
 *
 * <p>Every other ingester wires a client it was compiled against. This one takes its client, scope rules
 * and optionally its extractor from {@link org.hyland.contentlake.connector.ConnectorRegistry}, so a
 * connector jar can ingest without a module, a Dockerfile edit or a compose change.</p>
 */
@SpringBootApplication(scanBasePackages = {
        "org.hyland.contentlake",
        "org.hyland.contentlake.pluginhost"
})
public class PluginBatchIngesterApplication {

    public static void main(String[] args) {
        SpringApplication.run(PluginBatchIngesterApplication.class, args);
    }
}
