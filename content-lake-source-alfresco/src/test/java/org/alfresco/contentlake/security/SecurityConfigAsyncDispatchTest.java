package org.alfresco.contentlake.security;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.Callable;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.authentication;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest
@Import({SecurityConfig.class, SecurityConfigAsyncDispatchTest.AsyncController.class})
@TestPropertySource(properties = "content.service.url=http://localhost:8080")
class SecurityConfigAsyncDispatchTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void asyncRedispatch_allowsCompletionAfterInitialAuthentication() throws Exception {
        Authentication authentication = new UsernamePasswordAuthenticationToken(
                "admin",
                "admin",
                AuthorityUtils.createAuthorityList("ROLE_USER")
        );

        MvcResult mvcResult = mockMvc.perform(get("/api/test/async").with(authentication(authentication)))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isOk())
                .andExpect(content().string("ok"));
    }

    @Test
    void regularApiRequest_stillRequiresAuthentication() throws Exception {
        mockMvc.perform(get("/api/test/async"))
                .andExpect(status().isUnauthorized());
    }

    @RestController
    public static class AsyncController {

        @GetMapping("/api/test/async")
        Callable<ResponseEntity<String>> asyncEndpoint() {
            return () -> ResponseEntity.ok("ok");
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class TestApplication {
    }
}
