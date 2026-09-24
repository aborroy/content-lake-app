package org.hyland.contentlake.security;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CallerIdentityServiceTest {

    private final CallerIdentityService service = new CallerIdentityService(new SecurityContextService());

    @AfterEach
    void clearContext() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void readsTheIdentitiesOfACallerThatCarriesThem() {
        CallerIdentities carried = CallerIdentities.builder()
                .fallback(SourceIdentity.of("alfresco", "alice"))
                .add(SourceIdentity.of("nuxeo", "bob"))
                .build();

        assertThat(service.identities(new StubCallerAuthentication(carried))).isSameAs(carried);
    }

    @Test
    void treatsASingleCredentialAsOneUntypedIdentity() {
        Authentication single = new UsernamePasswordAuthenticationToken(
                "alice", null, List.of(new SimpleGrantedAuthority("ROLE_USER")));
        SecurityContextHolder.getContext().setAuthentication(single);

        CallerIdentities identities = service.identities(single);

        // Answers for every source type, which is what a repository-authenticated caller has always got.
        assertThat(identities.usernameFor("alfresco")).isEqualTo("alice");
        assertThat(identities.usernameFor("sharepoint")).isEqualTo("alice");
        assertThat(identities.describe()).isEqualTo("alice");
    }

    @Test
    void refusesToInventAPrincipalForAnAnonymousCaller() {
        Authentication anonymous = new AnonymousAuthenticationToken(
                "key", "anonymousUser", List.of(new SimpleGrantedAuthority("ROLE_ANONYMOUS")));
        SecurityContextHolder.getContext().setAuthentication(anonymous);

        // Must stay an AuthenticationException so it translates to 401 rather than a filter scoped to nobody.
        assertThatThrownBy(() -> service.identities(anonymous))
                .isInstanceOf(AuthenticationCredentialsNotFoundException.class);
    }

    @Test
    void refusesAnEmptyContext() {
        assertThatThrownBy(service::currentIdentities)
                .isInstanceOf(AuthenticationCredentialsNotFoundException.class);
    }

    @Test
    void readsTheCurrentCallerFromTheContext() {
        CallerIdentities carried = CallerIdentities.single("alice");
        SecurityContextHolder.getContext().setAuthentication(new StubCallerAuthentication(carried));

        assertThat(service.currentIdentities()).isSameAs(carried);
    }

    /** A caller that carries identities, standing in for whatever authenticated it. */
    private record StubCallerAuthentication(CallerIdentities identities) implements CallerAuthentication {

        @Override
        public Collection<? extends GrantedAuthority> getAuthorities() {
            return List.of(new SimpleGrantedAuthority("ROLE_USER"));
        }

        @Override
        public Object getCredentials() {
            return null;
        }

        @Override
        public Object getDetails() {
            return null;
        }

        @Override
        public Object getPrincipal() {
            return identities.primaryUsername();
        }

        @Override
        public boolean isAuthenticated() {
            return true;
        }

        @Override
        public void setAuthenticated(boolean isAuthenticated) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getName() {
            return identities.describe();
        }
    }
}
