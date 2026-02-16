package io.kestra.plugin.googleworkspace.chat;

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

import java.net.URI;

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
    @PluginProperty(dynamic = true)
    @NotBlank
    protected String url;

    @Schema(
        title = "JSON payload sent to Chat",
        description = "Raw JSON body sent to Chat"
    )
    protected Property<String> payload;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        String url = runContext.render(this.url);

        try (HttpClient client = new HttpClient(runContext, super.httpClientConfigurationWithOptions())) {
            String payload = runContext.render(this.payload).as(String.class).orElse(null);

            runContext.logger().debug("Send Discord webhook: {}", payload);
            HttpRequest request = HttpRequest.builder()
                .addHeader("Content-Type", "application/json")
                .uri(URI.create(url))
                .method("POST")
                .body(HttpRequest.StringRequestBody.builder()
                    .content(payload)
                    .build())
                .build();

            HttpResponse<String> response = client.request(request, String.class);

            runContext.logger().debug("Response: {}", response.getBody());

            if (response.getStatus().getCode() == 200) {
                runContext.logger().info("Request succeeded");
            }
        }
        return null;
    }
}
