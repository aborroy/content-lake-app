package org.hyland.contentlake.rag.security;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AlfrescoTicketAuthenticationFilterTest {

    @AfterEach
    void clearSecurityContext() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void authenticatesUiStyleTicketHeaderAndStripsAuthorizationFromDownstreamFilters() throws Exception {
        AuthenticationManager authenticationManager = mock(AuthenticationManager.class);
        var authenticated = new UsernamePasswordAuthenticationToken("rag-user", null);
        when(authenticationManager.authenticate(any())).thenReturn(authenticated);

        AlfrescoTicketAuthenticationFilter filter = new AlfrescoTicketAuthenticationFilter(authenticationManager);
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/rag/chat/stream");
        request.addHeader("Authorization", "Basic " + encode("TICKET_ui-demo:"));
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicBoolean chainCalled = new AtomicBoolean(false);
        AtomicReference<String> forwardedAuthorization = new AtomicReference<>("present");

        filter.doFilter(request, response, (req, res) -> {
            forwardedAuthorization.set(((jakarta.servlet.http.HttpServletRequest) req).getHeader("Authorization"));
            chainCalled.set(true);
        });

        var captor = org.mockito.ArgumentCaptor.forClass(UsernamePasswordAuthenticationToken.class);
        verify(authenticationManager).authenticate(captor.capture());
        assertThat(captor.getValue().getName()).isEqualTo("TICKET_ui-demo");
        assertThat(captor.getValue().getCredentials()).isEqualTo("");
        assertThat(SecurityContextHolder.getContext().getAuthentication()).isEqualTo(authenticated);
        assertThat(forwardedAuthorization.get()).isNull();
        assertThat(chainCalled).isTrue();
    }

    @Test
    void authenticatesTicketQueryParameter() throws Exception {
        AuthenticationManager authenticationManager = mock(AuthenticationManager.class);
        var authenticated = new UsernamePasswordAuthenticationToken("rag-user", null);
        when(authenticationManager.authenticate(any())).thenReturn(authenticated);

        AlfrescoTicketAuthenticationFilter filter = new AlfrescoTicketAuthenticationFilter(authenticationManager);
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/rag/search/semantic");
        request.setParameter("alf_ticket", "TICKET_query-demo");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, (req, res) -> { });

        var captor = org.mockito.ArgumentCaptor.forClass(UsernamePasswordAuthenticationToken.class);
        verify(authenticationManager).authenticate(captor.capture());
        assertThat(captor.getValue().getName()).isEqualTo("TICKET_query-demo");
        assertThat(SecurityContextHolder.getContext().getAuthentication()).isEqualTo(authenticated);
    }

    @Test
    void leavesStandardBasicAuthRequestsForSpringBasicAuthentication() throws Exception {
        AuthenticationManager authenticationManager = mock(AuthenticationManager.class);
        AlfrescoTicketAuthenticationFilter filter = new AlfrescoTicketAuthenticationFilter(authenticationManager);
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/rag/search/semantic");
        request.addHeader("Authorization", "Basic " + encode("admin:admin"));
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<String> forwardedAuthorization = new AtomicReference<>();

        filter.doFilter(request, response, (req, res) -> {
            forwardedAuthorization.set(((jakarta.servlet.http.HttpServletRequest) req).getHeader("Authorization"));
        });

        verify(authenticationManager, never()).authenticate(any());
        assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
        assertThat(forwardedAuthorization.get()).isEqualTo("Basic " + encode("admin:admin"));
    }

    private String encode(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
