package io.kestra.plugin.googleworkspace.chat;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import com.google.common.io.Files;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.server.EmbeddedServer;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@KestraTest
public class GoogleChatIncomingWebhookTest {

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(
            Map.of(
                "text", "Google test webhook notification"
            )
        );

        EmbeddedServer embeddedServer = applicationContext.getBean(EmbeddedServer.class);
        embeddedServer.start();

        GoogleChatIncomingWebhook task = GoogleChatIncomingWebhook.builder()
            .url(embeddedServer.getURI() + "/webhook-unit-test")
            .payload(
                Property.ofExpression(
                    Files.asCharSource(
                        new File(
                            Objects.requireNonNull(
                                GoogleChatIncomingWebhookTest.class.getClassLoader()
                                    .getResource("google-chat.peb")
                            )
                                .toURI()
                        ),
                        StandardCharsets.UTF_8
                    ).read()
                )
            )
            .build();

        task.run(runContext);

        assertThat(FakeWebhookController.data, containsString("Google test webhook notification"));
    }

    @Test
    void runViaChatApiPath() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        EmbeddedServer embeddedServer = applicationContext.getBean(EmbeddedServer.class);
        embeddedServer.start();

        FakeWebhookController.data = null;
        FakeWebhookController.queryParameters.clear();
        FakeWebhookController.headers.clear();

        GoogleChatIncomingWebhook task = GoogleChatIncomingWebhook.builder()
            .url(embeddedServer.getURI() + "/v1/spaces/AAAAtest/messages?key=test-key&token=test-token")
            .payload(Property.ofValue("{\"text\":\"sent through the Chat SDK\"}"))
            .build();

        task.run(runContext);

        // only the Google client library sends this, so it proves the SDK made the call and not the fallback
        assertThat(FakeWebhookController.headers, hasKey("x-goog-api-client"));
        assertThat(FakeWebhookController.data, containsString("sent through the Chat SDK"));
        assertThat(FakeWebhookController.queryParameters.get("key"), is("test-key"));
        assertThat(FakeWebhookController.queryParameters.get("token"), is("test-token"));
    }

    /** `options.headers` was rendered and then dropped before this change, so lock in that it is now sent. */
    @Test
    void sendsConfiguredHeaders() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        EmbeddedServer embeddedServer = applicationContext.getBean(EmbeddedServer.class);
        embeddedServer.start();

        FakeWebhookController.headers.clear();

        GoogleChatIncomingWebhook task = GoogleChatIncomingWebhook.builder()
            .url(embeddedServer.getURI() + "/v1/spaces/AAAAtest/messages?key=k&token=t")
            .payload(Property.ofValue("{\"text\":\"with a custom header\"}"))
            .options(
                AbstractChatConnection.RequestOptions.builder()
                    .headers(Property.ofValue(Map.of("X-Custom-Header", "kestra")))
                    .build()
            )
            .build();

        task.run(runContext);

        assertThat(FakeWebhookController.headers, hasKey("x-goog-api-client"));
        // the Google client lowercases header names, which HTTP treats as equivalent
        assertThat(FakeWebhookController.headers.get("x-custom-header"), is("kestra"));
    }

    /** A payload the Chat model cannot read must still be posted unchanged, as it was before the SDK. */
    @Test
    void postsAnUnparseablePayloadVerbatim() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        EmbeddedServer embeddedServer = applicationContext.getBean(EmbeddedServer.class);
        embeddedServer.start();

        for (String payload : new String[] { "not json at all", "[{\"text\":\"an array\"}]" }) {
            FakeWebhookController.data = null;
            FakeWebhookController.headers.clear();

            GoogleChatIncomingWebhook task = GoogleChatIncomingWebhook.builder()
                .url(embeddedServer.getURI() + "/v1/spaces/AAAAtest/messages?key=k&token=t")
                .payload(Property.ofValue(payload))
                .build();

            task.run(runContext);

            assertThat(FakeWebhookController.data, is(payload));
            assertThat(FakeWebhookController.headers, not(hasKey("x-goog-api-client")));
        }
    }

}
