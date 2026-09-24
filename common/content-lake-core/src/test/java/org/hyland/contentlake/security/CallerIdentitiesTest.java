package org.hyland.contentlake.security;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The behaviour-preservation lock for the caller-identity refactor.
 *
 * <p>The two shapes below are what the two-username authentication produced, so every answer asserted here
 * is the answer the code being replaced gave. If one of these changes, retrieval or rate limiting has
 * changed with it.</p>
 */
class CallerIdentitiesTest {

    @Nested
    class ASingleIdentity {

        @Test
        void answersForEverySourceTypeBecauseItIsUntyped() {
            CallerIdentities identities = CallerIdentities.single("alice");

            // What a repository-authenticated caller has always got: one name, used everywhere.
            assertThat(identities.usernameFor("alfresco")).isEqualTo("alice");
            assertThat(identities.usernameFor("nuxeo")).isEqualTo("alice");
            assertThat(identities.usernameFor("sharepoint")).isEqualTo("alice");
            assertThat(identities.usernameFor(null)).isEqualTo("alice");
        }

        @Test
        void describesItselfAsTheBareUsername() {
            // Load-bearing: this string is a rate-limit bucket key and a stored feedback author.
            assertThat(CallerIdentities.single("alice").describe()).isEqualTo("alice");
        }

        @Test
        void isItsOwnPrimary() {
            CallerIdentities identities = CallerIdentities.single("alice");

            assertThat(identities.primaryUsername()).isEqualTo("alice");
            assertThat(identities.size()).isEqualTo(1);
            assertThat(identities.isEmpty()).isFalse();
        }
    }

    @Nested
    class TwoRepositorySessions {

        private final CallerIdentities identities = CallerIdentities.builder()
                .fallback(SourceIdentity.of("alfresco", "alice"))
                .add(SourceIdentity.of("nuxeo", "bob"))
                .build();

        @Test
        void resolvesEachSourceToItsOwnIdentity() {
            assertThat(identities.usernameFor("alfresco")).isEqualTo("alice");
            assertThat(identities.usernameFor("nuxeo")).isEqualTo("bob");
        }

        @Test
        void sendsAThirdSourceTypeToTheFallback() {
            // Preserved deliberately. `isNuxeoSource(id) ? nuxeoUser : alfrescoUser` did this by having
            // nowhere else to send a third type; here it is a named choice rather than an accident.
            assertThat(identities.usernameFor("sharepoint")).isEqualTo("alice");
            assertThat(identities.usernameFor("cmis")).isEqualTo("alice");
        }

        @Test
        void describesItselfExactlyAsTheTwoUsernameTokenDid() {
            assertThat(identities.describe()).isEqualTo("alfresco:alice|nuxeo:bob");
        }

        @Test
        void ordersTheFallbackFirstWhateverTheBuildOrder() {
            CallerIdentities nuxeoAddedFirst = CallerIdentities.builder()
                    .add(SourceIdentity.of("nuxeo", "bob"))
                    .fallback(SourceIdentity.of("alfresco", "alice"))
                    .build();

            // describe() feeds a rate-limit bucket, so it cannot depend on the order a filter happened to
            // validate two credentials in.
            assertThat(nuxeoAddedFirst.describe()).isEqualTo("alfresco:alice|nuxeo:bob");
        }
    }

    @Nested
    class ACallerWithNoFallback {

        @Test
        void hasNoIdentityForAnUnclaimedSourceType() {
            CallerIdentities identities = CallerIdentities.builder()
                    .add(SourceIdentity.of("sharepoint", "alice@example.com"))
                    .build();

            assertThat(identities.usernameFor("sharepoint")).isEqualTo("alice@example.com");
            // Null means the source is dropped from the filter, not widened.
            assertThat(identities.usernameFor("alfresco")).isNull();
        }
    }

    @Nested
    class Normalisation {

        @Test
        void matchesASourceTypeWhateverItsCase() {
            CallerIdentities identities = CallerIdentities.builder()
                    .add(SourceIdentity.of("SharePoint", "alice@example.com"))
                    .build();

            assertThat(identities.usernameFor("sharepoint")).isEqualTo("alice@example.com");
            assertThat(identities.usernameFor("SHAREPOINT")).isEqualTo("alice@example.com");
        }

        @Test
        void refusesAnIdentityWithNoUsername() {
            assertThatThrownBy(() -> SourceIdentity.of("alfresco", " "))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("username");
        }

        @Test
        void treatsABlankSourceTypeAsUntyped() {
            assertThat(SourceIdentity.of(" ", "alice").isUntyped()).isTrue();
        }
    }

    @Nested
    class RefusingAmbiguity {

        @Test
        void refusesTwoIdentitiesForOneSourceType() {
            // Which name wins would decide what the caller reads, so it must not be settled by the order
            // two credentials were validated in.
            assertThatThrownBy(() -> CallerIdentities.builder()
                    .add(SourceIdentity.of("nuxeo", "bob"))
                    .add(SourceIdentity.of("nuxeo", "carol"))
                    .build())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("nuxeo");
        }

        @Test
        void refusesASecondFallback() {
            assertThatThrownBy(() -> CallerIdentities.builder()
                    .fallback(SourceIdentity.untyped("alice"))
                    .fallback(SourceIdentity.untyped("bob"))
                    .build())
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void refusesAnUntypedIdentityThatIsNotTheFallback() {
            // An untyped identity answers for everything, so adding it as one-among-many is meaningless.
            assertThatThrownBy(() -> CallerIdentities.builder().add(SourceIdentity.untyped("alice")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("fallback");
        }

        @Test
        void acceptsTheSameIdentityTwice() {
            CallerIdentities identities = CallerIdentities.builder()
                    .fallback(SourceIdentity.of("alfresco", "alice"))
                    .add(SourceIdentity.of("alfresco", "alice"))
                    .build();

            assertThat(identities.describe()).isEqualTo("alfresco:alice");
            assertThat(identities.size()).isEqualTo(1);
        }
    }

    @Nested
    class ScopeKey {

        @Test
        void cannotBeSpelledByAnotherCaller() {
            // The separator-joined form this replaces was not injective: a username containing the
            // separator produced another caller's key, and that key decides whose results are served.
            CallerIdentities spoofer = CallerIdentities.single("alice|nux:bob");
            CallerIdentities real = CallerIdentities.builder()
                    .fallback(SourceIdentity.of("alfresco", "alice"))
                    .add(SourceIdentity.of("nuxeo", "bob"))
                    .build();

            assertThat(spoofer.scopeKey()).isNotEqualTo(real.scopeKey());
        }

        @Test
        void distinguishesTwoCallersDifferingOnlyBySourceType() {
            CallerIdentities inAlfresco = CallerIdentities.builder()
                    .add(SourceIdentity.of("alfresco", "alice")).build();
            CallerIdentities inNuxeo = CallerIdentities.builder()
                    .add(SourceIdentity.of("nuxeo", "alice")).build();

            assertThat(inAlfresco.scopeKey()).isNotEqualTo(inNuxeo.scopeKey());
        }
    }

    @Nested
    class Empty {

        @Test
        void resolvesNothingAndDescribesNothing() {
            assertThat(CallerIdentities.EMPTY.isEmpty()).isTrue();
            assertThat(CallerIdentities.EMPTY.usernameFor("alfresco")).isNull();
            assertThat(CallerIdentities.EMPTY.primaryUsername()).isNull();
            assertThat(CallerIdentities.EMPTY.describe()).isEmpty();
        }

        @Test
        void isWhatAnEmptyBuilderProduces() {
            assertThat(CallerIdentities.builder().build()).isSameAs(CallerIdentities.EMPTY);
        }
    }
}
