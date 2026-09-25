package org.hyland.contentlake.rag.security;

import org.hyland.contentlake.security.CallerCredentials;
import org.hyland.contentlake.security.CallerIdentities;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the bearer filter does with an {@code Authorization} header, and what it leaves alone.
 *
 * <p>The cases that matter are the negative ones: a Basic credential must reach the filter after it untouched,
 * and a rejected token must not fall through to be retried as anonymous.</p>
 */
class BearerTokenAuthenticationFilterTest {

    /** Every request shape the filter handed to the manager. */
    private final List<PresentedCredentialsAuthentication> adjudicated = new ArrayList<>();

    @AfterEach
    void clearContext() {
        SecurityContextHolder.clearContext();
    }

    private AuthenticationManager accepting(String username) {
        return authentication -> {
            adjudicated.add((PresentedCredentialsAuthentication) authentication);
            return new MultiIdentityAuthentication(CallerIdentities.single(username));
        };
    }

    private AuthenticationManager rejecting() {
        return authentication -> {
            adjudicated.add((PresentedCredentialsAuthentication) authentication);
            throw new BadCredentialsException("Invalid or expired bearer token");
        };
    }

    @Test
    void passesTheTokenToTheChainAsAnAttributeWithNoClaimedPrincipal() throws Exception {
        MockHttpServletResponse response = run(bearerRequest("a.jwt.value"), accepting("alice"), true);

        assertThat(adjudicated).hasSize(1);
        CallerCredentials presented = adjudicated.get(0).credentials();
        assertThat(presented.attribute(BearerTokenAuthenticationFilter.BEARER_ATTRIBUTE))
                .isEqualTo("a.jwt.value");
        // The token is the only input: no caller-supplied name travels beside it.
        assertThat(presented.principal()).isEmpty();
        assertThat(SecurityContextHolder.getContext().getAuthentication().getName()).isEqualTo("alice");
        assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    void answers401AndStopsTheChainWhenTheTokenIsRejected() throws Exception {
        MockHttpServletResponse response = run(bearerRequest("expired"), rejecting(), false);

        // Not a fall-through. A rejected token retried as anonymous would reach a permitAll endpoint and read
        // as the token having worked.
        assertThat(response.getStatus()).isEqualTo(401);
        assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
    }

    @Test
    void leavesABasicCredentialEntirelyAlone() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/rag/search/semantic");
        request.addHeader("Authorization", "Basic YWRtaW46YWRtaW4=");

        MockHttpServletResponse response = run(request, rejecting(), true);

        // Never adjudicated here, so BasicAuthenticationFilter sees the header exactly as sent.
        assertThat(adjudicated).isEmpty();
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
    }

    @Test
    void ignoresARequestWithNoAuthorizationHeader() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/rag/health");

        run(request, rejecting(), true);

        assertThat(adjudicated).isEmpty();
    }

    @Test
    void ignoresAnEmptyBearerValue() throws Exception {
        run(bearerRequest("   "), rejecting(), true);

        // Nothing to validate, so this is not a rejected token; it falls through as unauthenticated.
        assertThat(adjudicated).isEmpty();
    }

    @Test
    void doesNotReAuthenticateAnAlreadyAuthenticatedRequest() throws Exception {
        SecurityContextHolder.getContext().setAuthentication(
                new MultiIdentityAuthentication(CallerIdentities.single("already")));

        run(bearerRequest("a.jwt.value"), rejecting(), true);

        assertThat(adjudicated).isEmpty();
        assertThat(SecurityContextHolder.getContext().getAuthentication().getName()).isEqualTo("already");
    }

    private static MockHttpServletRequest bearerRequest(String token) {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/rag/search/semantic");
        request.addHeader("Authorization", "Bearer " + token);
        return request;
    }

    /** Runs the filter and asserts whether the chain continued. */
    private MockHttpServletResponse run(MockHttpServletRequest request,
                                        AuthenticationManager manager,
                                        boolean expectChainContinued) throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        boolean[] continued = {false};

        new BearerTokenAuthenticationFilter(manager)
                .doFilter(request, response, (req, res) -> continued[0] = true);

        assertThat(continued[0]).isEqualTo(expectChainContinued);
        return response;
    }
}
