package org.hyland.contentlake.rag.security;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.hyland.contentlake.security.GroupResolutionFailurePolicy;
import org.hyland.contentlake.security.SourceGroupResolver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SourceGroupResolverRegistryTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger logger;

    @AfterEach
    void detachAppender() {
        if (logger != null && appender != null) {
            logger.detachAppender(appender);
        }
    }

    // -----------------------------------------------------------------------
    // Resolver selection
    // -----------------------------------------------------------------------

    @Test
    void selectsTheResolverClaimingTheSourceType() {
        SourceGroupResolverRegistry registry = uncached(
                resolver("alfresco", () -> List.of("GROUP_A")),
                resolver("nuxeo", () -> List.of("GROUP_N")));

        assertThat(registry.authorities("alice", "repo", "nuxeo"))
                .containsExactly("alice", "GROUP_EVERYONE", "GROUP_N");
    }

    @Test
    void matchesTheSourceTypeIgnoringCaseAndSurroundingSpace() {
        SourceGroupResolverRegistry registry = uncached(resolver(" Alfresco ", () -> List.of("GROUP_A")));

        assertThat(registry.authorities("alice", "repo", "ALFRESCO"))
                .contains("GROUP_A");
        assertThat(registry.canExpandGroups("alfresco")).isTrue();
        assertThat(registry.resolvableSourceTypes()).containsExactly("alfresco");
    }

    @Test
    void noResolverForTheType_resolvesDefaultsAndConsultsNothing() {
        AtomicInteger calls = new AtomicInteger();
        SourceGroupResolverRegistry registry = uncached(resolver("alfresco", () -> {
            calls.incrementAndGet();
            return List.of("GROUP_A");
        }));

        assertThat(registry.authorities("alice", "docbase-1", "cmis"))
                .containsExactly("alice", "GROUP_EVERYONE");
        assertThat(calls).hasValue(0);
        assertThat(registry.canExpandGroups("cmis")).isFalse();
    }

    @Test
    void unknownSourceType_resolvesDefaults() {
        SourceGroupResolverRegistry registry = uncached(resolver("alfresco", () -> List.of("GROUP_A")));

        assertThat(registry.authorities("alice", "mystery", null))
                .containsExactly("alice", "GROUP_EVERYONE");
    }

    @Test
    void withoutResolvers_resolvesDefaultsForEveryType() {
        assertThat(SourceGroupResolverRegistry.withoutResolvers().authorities("alice", "repo", "alfresco"))
                .containsExactly("alice", "GROUP_EVERYONE");
    }

    @Test
    void twoResolversClaimingOneType_refuseToStart() {
        // Which of the two wins decides who can read what, so bean ordering must not settle it.
        assertThatThrownBy(() -> uncached(
                resolver("alfresco", List::of),
                resolver("ALFRESCO", List::of)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("alfresco");
    }

    @Test
    void aResolverWithABlankSourceType_refusesToStart() {
        assertThatThrownBy(() -> uncached(resolver("  ", List::of)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("blank sourceType");
    }

    // -----------------------------------------------------------------------
    // The three answers a resolver can give
    // -----------------------------------------------------------------------

    @Test
    void emptyGroupList_isAKnownAnswerAndKeepsTheSource() {
        SourceGroupResolverRegistry registry = uncached(resolver("alfresco", List::of));

        assertThat(registry.authorities("alice", "repo", "alfresco"))
                .containsExactly("alice", "GROUP_EVERYONE");
    }

    @Test
    void nullGroupList_isNoIdentityHereAndKeepsTheSource() {
        SourceGroupResolverRegistry registry = uncached(resolver("alfresco", () -> null));

        assertThat(registry.authorities("alice", "repo", "alfresco"))
                .containsExactly("alice", "GROUP_EVERYONE");
    }

    @Test
    void aThrownException_isADirectoryFailureAndFollowsThePolicy() {
        SourceGroupResolver failing = resolver("alfresco", () -> {
            throw new IllegalStateException("directory down");
        });

        assertThat(uncached(GroupResolutionFailurePolicy.FAIL_CLOSED, failing)
                .authorities("alice", "repo", "alfresco"))
                .isEmpty();
        assertThat(uncached(GroupResolutionFailurePolicy.DEGRADE, failing)
                .authorities("alice", "repo", "alfresco"))
                .containsExactly("alice", "GROUP_EVERYONE");
    }

    @Test
    void theCallersOwnNameIsNeverLostToADuplicateGroup() {
        SourceGroupResolverRegistry registry =
                uncached(resolver("alfresco", () -> List.of("GROUP_EVERYONE", "alice", "GROUP_A")));

        assertThat(registry.authorities("alice", "repo", "alfresco"))
                .containsExactly("alice", "GROUP_EVERYONE", "GROUP_A");
    }

    // -----------------------------------------------------------------------
    // Warn-once
    // -----------------------------------------------------------------------

    @Test
    void anUnresolvableSourceIsReportedOncePerSource() {
        captureLogs();
        SourceGroupResolverRegistry registry = uncached(resolver("alfresco", List::of));

        registry.authorities("alice", "docbase-1", "cmis");
        registry.authorities("bob", "docbase-1", "cmis");
        registry.authorities("alice", "docbase-2", "cmis");

        assertThat(warnings("has no group resolver")).hasSize(2);
    }

    @Test
    void aBlankSourceIdIsNotReported() {
        captureLogs();
        SourceGroupResolverRegistry registry = uncached(resolver("alfresco", List::of));

        registry.authorities("alice", "  ", "cmis");
        registry.authorities("alice", null, "cmis");

        assertThat(warnings("has no group resolver")).isEmpty();
    }

    // -----------------------------------------------------------------------
    // Cache
    // -----------------------------------------------------------------------

    @Test
    void aSecondLookupForTheSameUserAndTypeIsServedFromTheCache() {
        AtomicInteger calls = new AtomicInteger();
        SourceGroupResolverRegistry registry = cached(60L, resolver("alfresco", () -> {
            calls.incrementAndGet();
            return List.of("GROUP_A");
        }));

        registry.authorities("alice", "repo", "alfresco");
        registry.authorities("alice", "repo", "alfresco");

        assertThat(calls).hasValue(1);
    }

    @Test
    void theCacheIsKeyedOnTheUserAsWellAsTheType() {
        // Sharing one entry across callers would hand one caller another's group membership.
        AtomicInteger calls = new AtomicInteger();
        SourceGroupResolverRegistry registry = cached(60L, resolverPerUser(calls));

        assertThat(registry.authorities("alice", "repo", "alfresco")).contains("GROUP_alice");
        assertThat(registry.authorities("bob", "repo", "alfresco")).contains("GROUP_bob");
        assertThat(calls).hasValue(2);
    }

    @Test
    void oneEntryServesEverySourceOfTheSameType() {
        // Membership is a property of the directory, not of the source id, and two Alfresco source ids
        // resolved against the same repository would otherwise pay two round trips.
        AtomicInteger calls = new AtomicInteger();
        SourceGroupResolverRegistry registry = cached(60L, resolver("alfresco", () -> {
            calls.incrementAndGet();
            return List.of("GROUP_A");
        }));

        registry.authorities("alice", "repo-1", "alfresco");
        registry.authorities("alice", "repo-2", "alfresco");

        assertThat(calls).hasValue(1);
    }

    @Test
    void aTtlOfZeroDisablesTheCache() {
        AtomicInteger calls = new AtomicInteger();
        SourceGroupResolverRegistry registry = cached(0L, resolver("alfresco", () -> {
            calls.incrementAndGet();
            return List.of("GROUP_A");
        }));

        registry.authorities("alice", "repo", "alfresco");
        registry.authorities("alice", "repo", "alfresco");

        assertThat(calls).hasValue(2);
    }

    @Test
    void aFailureIsNotCached() {
        // A directory outage must be retried, not held for the whole TTL.
        AtomicInteger calls = new AtomicInteger();
        SourceGroupResolverRegistry registry = cached(60L, resolver("alfresco", () -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("directory down");
            }
            return List.of("GROUP_A");
        }));

        assertThat(registry.authorities("alice", "repo", "alfresco")).isEmpty();
        assertThat(registry.authorities("alice", "repo", "alfresco")).contains("GROUP_A");
    }

    @Test
    void noIdentityIsCachedAsSuchRatherThanAsNoGroups() {
        // The two answers differ, and a negative answer cached as an empty group list would be
        // indistinguishable from a known user in no groups on the next read.
        AtomicInteger calls = new AtomicInteger();
        SourceGroupResolverRegistry registry = cached(60L, resolver("alfresco", () -> {
            calls.incrementAndGet();
            return null;
        }));

        assertThat(registry.authorities("alice", "repo", "alfresco"))
                .containsExactly("alice", "GROUP_EVERYONE");
        assertThat(registry.authorities("alice", "repo", "alfresco"))
                .containsExactly("alice", "GROUP_EVERYONE");
        assertThat(calls).hasValue(1);
    }

    @Test
    void theCacheIsMeteredWhenAMeterRegistryIsAvailable() {
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        SourceGroupResolverRegistry registry = new SourceGroupResolverRegistry(
                List.of(resolver("alfresco", () -> List.of("GROUP_A"))),
                GroupResolutionFailurePolicy.FAIL_CLOSED, 60L, 100L, meterRegistry);

        registry.authorities("alice", "repo", "alfresco");

        // CaffeineCacheMetrics names the meters cache.* and carries the cache name as a tag.
        assertThat(meterRegistry.getMeters())
                .anyMatch(meter -> "rag.security.groups".equals(meter.getId().getTag("cache")));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static SourceGroupResolverRegistry uncached(SourceGroupResolver... resolvers) {
        return uncached(GroupResolutionFailurePolicy.FAIL_CLOSED, resolvers);
    }

    private static SourceGroupResolverRegistry uncached(GroupResolutionFailurePolicy policy,
                                                        SourceGroupResolver... resolvers) {
        return new SourceGroupResolverRegistry(List.of(resolvers), policy, 0L, 0L, null);
    }

    private static SourceGroupResolverRegistry cached(long ttlSeconds, SourceGroupResolver... resolvers) {
        return new SourceGroupResolverRegistry(List.of(resolvers),
                GroupResolutionFailurePolicy.FAIL_CLOSED, ttlSeconds, 100L, null);
    }

    private static SourceGroupResolver resolver(String sourceType, Supplier<List<String>> answer) {
        return new SourceGroupResolver() {
            @Override
            public String sourceType() {
                return sourceType;
            }

            @Override
            public List<String> resolveGroups(String username) {
                return answer.get();
            }
        };
    }

    /** A resolver whose answer depends on the caller, so a shared cache entry would be visible. */
    private static SourceGroupResolver resolverPerUser(AtomicInteger calls) {
        return new SourceGroupResolver() {
            @Override
            public String sourceType() {
                return "alfresco";
            }

            @Override
            public List<String> resolveGroups(String username) {
                calls.incrementAndGet();
                return List.of("GROUP_" + username);
            }
        };
    }

    private void captureLogs() {
        logger = (Logger) LoggerFactory.getLogger(SourceGroupResolverRegistry.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    private List<ILoggingEvent> warnings(String fragment) {
        return appender.list.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .filter(event -> event.getFormattedMessage().contains(fragment))
                .toList();
    }
}
