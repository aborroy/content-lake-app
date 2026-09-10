package org.hyland.contentlake.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides the classic Jackson 2 {@link ObjectMapper} the core HXPR clients are written against.
 *
 * <p>Spring Boot 4's {@code JacksonAutoConfiguration} targets Jackson 3 ({@code tools.jackson}) via
 * {@code spring-boot-starter-jackson} and no longer registers a
 * {@code com.fasterxml.jackson.databind.ObjectMapper} bean, so core supplies one explicitly. Guarded
 * with {@link ConditionalOnMissingBean} so an application module defining its own wins.</p>
 *
 * <p>{@link JavaTimeModule} is registered because a bean that cannot handle {@code java.time} is a
 * trap: Boot's own mapper always had the module, so code injecting this one instead inherits a mapper
 * that fails on the first {@code OffsetDateTime} it meets, and Jackson's
 * {@code REQUIRE_HANDLERS_FOR_JAVA8_TIMES} makes that a thrown exception rather than a numeric
 * fallback.</p>
 */
@Configuration
public class ContentLakeObjectMapperConfig {

    @Bean
    @ConditionalOnMissingBean(ObjectMapper.class)
    public ObjectMapper contentLakeObjectMapper() {
        return JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .build();
    }
}
