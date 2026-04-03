package org.hyland.contentlake.rag.security;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class NuxeoTokenAuthenticationFilterTest {

    @AfterEach
    void clearSecurityContext() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void authenticatesRequestFromNuxeoTokenHeader() throws Exception {
        AuthenticationManager authenticationManager = mock(AuthenticationManager.class);
        var authenticated = new UsernamePasswordAuthenticationToken("rag-user", null);
        when(authenticationManager.authenticate(any())).thenReturn(authenticated);

        NuxeoTokenAuthenticationFilter filter = new NuxeoTokenAuthenticationFilter(authenticationManager);
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/rag/search/semantic");
        request.addHeader("X-Authentication-Token", "token-123");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicBoolean chainCalled = new AtomicBoolean(false);

        filter.doFilter(request, response, (req, res) -> chainCalled.set(true));

        var captor = org.mockito.ArgumentCaptor.forClass(UsernamePasswordAuthenticationToken.class);
        verify(authenticationManager).authenticate(captor.capture());
        assertThat(captor.getValue().getName())
                .isEqualTo(MultiSourceAuthenticationProvider.NUXEO_TOKEN_PRINCIPAL_PREFIX + "token-123");
        assertThat(SecurityContextHolder.getContext().getAuthentication()).isEqualTo(authenticated);
        assertThat(chainCalled).isTrue();
    }

    @Test
    void skipsNuxeoTokenWhenBasicAuthorizationHeaderIsPresent() throws Exception {
        AuthenticationManager authenticationManager = mock(AuthenticationManager.class);
        NuxeoTokenAuthenticationFilter filter = new NuxeoTokenAuthenticationFilter(authenticationManager);
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/rag/search/semantic");
        request.addHeader("X-Authentication-Token", "token-123");
        request.addHeader("Authorization", "Basic Zm9vOmJhcg==");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicBoolean chainCalled = new AtomicBoolean(false);

        filter.doFilter(request, response, (req, res) -> chainCalled.set(true));

        verify(authenticationManager, never()).authenticate(any());
        assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
        assertThat(chainCalled).isTrue();
    }
}
