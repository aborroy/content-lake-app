package org.hyland.contentlake.rag;

import org.hyland.contentlake.connector.ConnectorSchemaController;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.AutoConfigurationExcludeFilter;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.TypeExcludeFilter;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

/**
 * The RAG service.
 *
 * <p>The scan is declared here rather than through {@code @SpringBootApplication(scanBasePackages)} so it can
 * carry an exclude filter. {@link ConnectorSchemaController} is a {@code @Controller} in core -- Spring
 * Framework 7 maps a handler only if it carries that stereotype -- and core is scanned by every application
 * here, so without this exclusion the connector endpoints would appear on a service that ingests from nothing
 * and has no connector to describe. The ingesters get them by scanning core; this one opts out.</p>
 *
 * <p>The two {@code CUSTOM} filters are not optional decoration. An explicit {@code @ComponentScan} replaces
 * the one {@code @SpringBootApplication} contributes, including its default excludes, so leaving them out
 * silently drops {@link TypeExcludeFilter} (which is how Boot's test slices narrow a scan) and
 * {@link AutoConfigurationExcludeFilter} (which stops an auto-configuration class from also being picked up as
 * a plain configuration). Restating them keeps this scan equivalent to the annotation it replaces.</p>
 */
@SpringBootApplication
@ComponentScan(
        basePackages = "org.hyland.contentlake",
        excludeFilters = {
                @ComponentScan.Filter(type = FilterType.CUSTOM, classes = TypeExcludeFilter.class),
                @ComponentScan.Filter(type = FilterType.CUSTOM, classes = AutoConfigurationExcludeFilter.class),
                @ComponentScan.Filter(
                        type = FilterType.ASSIGNABLE_TYPE,
                        classes = ConnectorSchemaController.class)
        })
public class RagServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(RagServiceApplication.class, args);
    }
}
