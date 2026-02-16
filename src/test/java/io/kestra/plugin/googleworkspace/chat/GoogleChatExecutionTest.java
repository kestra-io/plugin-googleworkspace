package io.kestra.plugin.googleworkspace.chat;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.TestRunner;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

@KestraTest
public class GoogleChatExecutionTest extends AbstractChatTest {

    @Inject
    protected TestRunner runner;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @BeforeEach
    protected void init() throws IOException, URISyntaxException {
        FakeWebhookController.data = null;

        repositoryLoader.load(Objects.requireNonNull(getClass().getClassLoader().getResource("flows/common")));
        repositoryLoader.load(Objects.requireNonNull(getClass().getClassLoader().getResource("flows/chat")));
        this.runner.run();
    }

    @Test
    void flow() throws Exception {
        var failedExecution = runAndCaptureExecution(
            "main-flow-that-fails",
            "google"
        );

        String receivedData = waitForWebhookData(() -> FakeWebhookController.data,5000);

        assertThat(receivedData, containsString("https://mysuperhost.com/kestra/ui"));
        assertThat(receivedData, containsString(failedExecution.getId()));
        assertThat(receivedData, containsString("Failed on task `failed`"));
        assertThat(receivedData, containsString("Final task ID failed"));
        assertThat(receivedData, containsString("Kestra Google notification"));
    }

    @Test
    void flowSuccessful() throws Exception {
        var execution = runAndCaptureExecution(
            "main-flow-that-succeeds",
            "google-successful"
        );

        String receivedData = waitForWebhookData(() -> FakeWebhookController.data,5000);

        assertThat(receivedData, containsString("https://mysuperhost.com/kestra/ui"));
        assertThat(receivedData, containsString(execution.getId()));
        assertThat(FakeWebhookController.data, not(containsString("Failed on task `failed`")));
        assertThat(FakeWebhookController.data, containsString("Final task ID success"));
        assertThat(receivedData, containsString("Kestra Google notification"));
    }
}
