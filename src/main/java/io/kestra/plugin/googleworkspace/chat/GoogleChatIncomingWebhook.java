package io.kestra.plugin.googleworkspace.chat;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.chat.v1.HangoutsChat;
import com.google.api.services.chat.v1.model.Message;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Send Google Chat message via webhook",
    description = "Posts JSON to a Google Chat incoming webhook. Commonly used in `errors` handlers for flow-level alerts. Configure the webhook in Chat first; no OAuth needed."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a Google Chat notification on a failed flow execution.",
            full = true,
            code = """
                id: unreliable_flow
                namespace: company.team

                tasks:
                  - id: fail
                    type: io.kestra.plugin.scripts.shell.Commands
                    runner: PROCESS
                    commands:
                      - exit 1

                errors:
                  - id: alert_on_failure
                    type: io.kestra.plugin.googleworkspace.chat.GoogleChatIncomingWebhook
                    url: "{{ secret('GOOGLE_WEBHOOK') }}" # https://chat.googleapis.com/v1/spaces/xzy/messages?threadKey=errorThread
                    payload: |
                      {
                        "text": "Google Chat Alert"
                      }
                """
        ),
        @Example(
            title = "Send a Google Chat message via incoming webhook.",
            full = true,
            code = """
                id: google_incoming_webhook
                namespace: company.team

                tasks:
                  - id: send_google_chat_message
                    type: io.kestra.plugin.googleworkspace.chat.GoogleChatIncomingWebhook
                    url: "{{ secret('GOOGLE_WEBHOOK') }}"
                    payload: |
                      {
                        "text": "Google Chat Hello"
                      }
                """
        ),
    },
    aliases = "io.kestra.plugin.notifications.google.GoogleChatIncomingWebhook"
)
public class GoogleChatIncomingWebhook extends AbstractChatConnection {

    @Schema(
        title = "Incoming Google Chat webhook URL",
        description = "Full Chat webhook endpoint (e.g. https://chat.googleapis.com/v1/spaces/.../messages); threadKey may be included"
    )
    @PluginProperty(dynamic = true, group = "main")
    @NotBlank
    protected String url;

    @Schema(
        title = "JSON payload sent to Chat",
        description = "Raw JSON body sent to Chat"
    )
    @PluginProperty(group = "main")
    protected Property<String> payload;

    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    /** A Chat webhook URL is the spaces.messages.create path with its credentials in the query string. */
    private static final Pattern WEBHOOK_PATH = Pattern.compile("^/v1/(spaces/[^/]+)/messages/?$");

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        final Logger logger = runContext.logger();

        String rUrl = runContext.render(this.url);
        String rPayload = runContext.render(this.payload).as(String.class).orElse(null);

        logger.debug("Send Google Chat webhook: {}", rPayload);

        URI uri = URI.create(rUrl);
        Matcher matcher = WEBHOOK_PATH.matcher(uri.getPath() == null ? "" : uri.getPath());

        Message message = matcher.matches() ? message(rPayload) : null;

        if (message == null) {
            // Google treats the webhook URL as opaque, so anything we cannot read stays a verbatim POST
            logger.debug("URL or payload is not in the Chat API shape, posting it as-is");
            this.post(runContext, rUrl, rPayload);
        } else {
            this.send(runContext, uri, matcher.group(1), message);
        }

        return null;
    }

    /** Returns null when the payload is not a Chat message object, leaving the caller to post it unchanged. */
    private static Message message(String payload) {
        if (payload == null) {
            return null;
        }

        try {
            return JSON_FACTORY.createJsonParser(payload).parse(Message.class);
        } catch (Exception e) {
            return null;
        }
    }

    private void send(RunContext runContext, URI uri, String space, Message message) throws Exception {
        HangoutsChat chat = new HangoutsChat.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            JSON_FACTORY,
            this.requestInitializer(runContext)
        )
            .setApplicationName("Kestra")
            .setRootUrl(uri.getScheme() + "://" + uri.getAuthority() + "/")
            .build();

        HangoutsChat.Spaces.Messages.Create create = chat.spaces().messages().create(space, message);
        queryParameters(uri).forEach(create::set);

        Message sent = create.execute();

        runContext.logger().info("Google Chat message sent ({})", sent.getName());
    }

    /** Carries the `options` timeouts and headers over to the Google transport. */
    private HttpRequestInitializer requestInitializer(RunContext runContext) throws Exception {
        if (this.options == null) {
            return request -> request.setReadTimeout((int) DEFAULT_READ_TIMEOUT.toMillis());
        }

        var rConnectTimeout = runContext.render(this.options.getConnectTimeout()).as(Duration.class);
        var rReadTimeout = runContext.render(this.options.getReadIdleTimeout()).as(Duration.class);
        Map<String, String> rHeaders = this.options.getHeaders() == null
            ? Map.of()
            : runContext.render(this.options.getHeaders()).asMap(String.class, String.class);

        return request -> {
            rConnectTimeout.ifPresent(timeout -> request.setConnectTimeout((int) timeout.toMillis()));
            request.setReadTimeout((int) rReadTimeout.orElse(DEFAULT_READ_TIMEOUT).toMillis());
            rHeaders.forEach((name, value) -> request.getHeaders().set(name, value));
        };
    }

    private static Map<String, String> queryParameters(URI uri) {
        Map<String, String> parameters = new LinkedHashMap<>();

        if (uri.getRawQuery() == null) {
            return parameters;
        }

        for (String pair : uri.getRawQuery().split("&")) {
            int separator = pair.indexOf('=');
            if (separator > 0) {
                parameters.put(
                    URLDecoder.decode(pair.substring(0, separator), StandardCharsets.UTF_8),
                    URLDecoder.decode(pair.substring(separator + 1), StandardCharsets.UTF_8)
                );
            }
        }

        return parameters;
    }

    private void post(RunContext runContext, String url, String payload) throws Exception {
        try (HttpClient client = new HttpClient(runContext, super.httpClientConfigurationWithOptions())) {
            HttpRequest request = super.createRequestBuilder(runContext)
                .addHeader("Content-Type", "application/json")
                .uri(URI.create(url))
                .method("POST")
                .body(
                    HttpRequest.StringRequestBody.builder()
                        .content(payload)
                        .build()
                )
                .build();

            HttpResponse<String> response = client.request(request, String.class);

            runContext.logger().debug("Response: {}", response.getBody());

            if (response.getStatus().getCode() != 200) {
                throw new IllegalStateException(
                    "Google Chat webhook failed with HTTP " + response.getStatus().getCode() + ": " + response.getBody()
                );
            }

            runContext.logger().info("Request succeeded");
        }
    }
}
