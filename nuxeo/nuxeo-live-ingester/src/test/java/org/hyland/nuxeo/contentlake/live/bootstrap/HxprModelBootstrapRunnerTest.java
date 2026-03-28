package org.hyland.nuxeo.contentlake.live.bootstrap;

import org.hyland.contentlake.client.HxprModelProvisioner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class HxprModelBootstrapRunnerTest {

    @Mock
    private HxprModelProvisioner modelProvisioner;

    private HxprModelBootstrapRunner runner;

    @BeforeEach
    void setUp() {
        runner = new HxprModelBootstrapRunner(modelProvisioner);
    }

    @Test
    void run_bootstrapsModelWhenEnabled() throws Exception {
        ReflectionTestUtils.setField(runner, "enabled", true);
        ReflectionTestUtils.setField(runner, "fragmentsLocation", "classpath:model-fragments.json");

        runner.run(new DefaultApplicationArguments(new String[0]));

        verify(modelProvisioner).ensureModelPresent("classpath:model-fragments.json");
    }

    @Test
    void run_skipsBootstrapWhenDisabled() throws Exception {
        ReflectionTestUtils.setField(runner, "enabled", false);
        ReflectionTestUtils.setField(runner, "fragmentsLocation", "classpath:model-fragments.json");

        runner.run(new DefaultApplicationArguments(new String[0]));

        verify(modelProvisioner, never()).ensureModelPresent("classpath:model-fragments.json");
    }
}
