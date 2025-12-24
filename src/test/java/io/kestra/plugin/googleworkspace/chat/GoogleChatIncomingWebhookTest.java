package io.kestra.plugin.googleworkspace.chat;


import com.google.common.io.Files;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.server.EmbeddedServer;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@KestraTest
public class GoogleChatIncomingWebhookTest {

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of(
            "text", "Google test webhook notification"
                                                           ));

        EmbeddedServer embeddedServer = applicationContext.getBean(EmbeddedServer.class);
        embeddedServer.start();

        GoogleChatIncomingWebhook task = GoogleChatIncomingWebhook.builder()
            .url(embeddedServer.getURI() + "/webhook-unit-test")
            .payload(new Property<>(
                Files.asCharSource(
                    new File(Objects.requireNonNull(GoogleChatIncomingWebhookTest.class.getClassLoader()
                            .getResource("google-chat.peb"))
                        .toURI()),
                    StandardCharsets.UTF_8
                                  ).read()
                    ))
            .build();

        task.run(runContext);

        assertThat(FakeWebhookController.data, containsString("Google test webhook notification"));
    }

}
