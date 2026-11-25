package io.kestra.plugin.googleworkspace.chat;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.google.common.io.Files;
import com.sun.net.httpserver.HttpServer;

import io.kestra.core.http.client.configurations.ProxyConfiguration;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.utils.IdUtils;

import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.server.EmbeddedServer;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class GoogleChatIncomingWebhookTest {

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ModelValidator modelValidator;

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

    /** The fallback path must fail on a non-2xx exactly as it did before the SDK, this pins that. */
    @Test
    void verbatimPathFailsOnNonSuccess() {
        RunContext runContext = runContextFactory.of(Map.of());
        EmbeddedServer embeddedServer = applicationContext.getBean(EmbeddedServer.class);
        embeddedServer.start();

        GoogleChatIncomingWebhook task = GoogleChatIncomingWebhook.builder()
            .url(embeddedServer.getURI() + "/webhook-unit-test/reject")
            .payload(Property.ofValue("{\"text\":\"rejected\"}"))
            .build();

        assertThrows(Exception.class, () -> task.run(runContext));
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

    @Test
    void sendsThroughAnAuthenticatedProxy() throws Exception {
        var expected = "Basic %s".formatted(Base64.getEncoder().encodeToString("kestra:s3cret".getBytes(StandardCharsets.UTF_8)));
        var seenUri = new AtomicReference<String>();
        var seenBody = new AtomicReference<String>();

        var proxy = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        proxy.createContext("/", exchange ->
        {
            var auth = exchange.getRequestHeaders().getFirst("Proxy-Authorization");
            if (!expected.equals(auth)) {
                exchange.getResponseHeaders().add("Proxy-Authenticate", "Basic realm=\"x\"");
                exchange.sendResponseHeaders(407, -1);
            } else {
                seenUri.set(exchange.getRequestURI().toString());
                seenBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                exchange.sendResponseHeaders(200, -1);
            }
            exchange.close();
        });
        proxy.start();

        try {
            var task = GoogleChatIncomingWebhook.builder()
                .url("http://chat.example.test/v1/spaces/AAAA/messages?key=k&token=t")
                .payload(Property.ofValue("{\"text\":\"hi\"}"))
                .options(
                    AbstractChatConnection.RequestOptions.builder()
                        .proxy(
                            ProxyConfiguration.builder()
                                .type(Property.ofValue(Proxy.Type.HTTP))
                                .address(Property.ofValue("127.0.0.1"))
                                .port(Property.ofValue(proxy.getAddress().getPort()))
                                .username(Property.ofValue("kestra"))
                                .password(Property.ofValue("s3cret"))
                                .build()
                        )
                        .build()
                )
                .build();

            task.run(runContextFactory.of(Map.of()));

            assertThat(seenUri.get(), is("http://chat.example.test/v1/spaces/AAAA/messages?key=k&token=t"));
            assertThat(seenBody.get(), is("{\"text\":\"hi\"}"));
        } finally {
            proxy.stop(0);
        }
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

            var task = GoogleChatIncomingWebhook.builder()
                .url(embeddedServer.getURI() + "/v1/spaces/AAAAtest/messages?key=k&token=t")
                .payload(Property.ofValue(payload))
                .build();

            task.run(runContext);

            assertThat(FakeWebhookController.data, is(payload));
            assertThat(FakeWebhookController.headers, not(hasKey("x-goog-api-client")));
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    void flowSettingDeprecatedOptionsStillValidatesAndRuns() throws Exception {
        var embeddedServer = applicationContext.getBean(EmbeddedServer.class);
        embeddedServer.start();

        var message = "deprecated-options-" + IdUtils.create();
        var flow = YamlParser.parse(
            """
                id: googlechat_deprecated_options
                namespace: io.kestra.tests
                tasks:
                  - id: webhook
                    type: io.kestra.plugin.googleworkspace.chat.GoogleChatIncomingWebhook
                    url: "%s/webhook-unit-test"
                    payload: '{"text": "%s"}'
                    options:
                      connectionPoolIdleTimeout: PT10S
                      maxContentLength: 1024
                """.formatted(embeddedServer.getURI(), message),
            Flow.class
        );

        assertThat(modelValidator.isValid(flow).isEmpty(), is(true));

        var runContext = runContextFactory.of(Map.of());
        var task = (GoogleChatIncomingWebhook) flow.getTasks().getFirst();
        var options = task.getOptions();
        assertThat(runContext.render(options.getConnectionPoolIdleTimeout()).as(Duration.class).orElseThrow(), is(Duration.ofSeconds(10)));
        assertThat(runContext.render(options.getMaxContentLength()).as(Integer.class).orElseThrow(), is(1024));

        task.run(runContext);

        assertThat(FakeWebhookController.data, containsString(message));
    }
}
