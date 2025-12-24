package io.kestra.plugin.googleworkspace.chat;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.plugins.notifications.ExecutionInterface;
import io.kestra.core.plugins.notifications.ExecutionService;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Send a Google Chat message with the execution information.",
    description = "The message will include a link to the execution page in the UI along with the execution ID, namespace, flow name, the start date, duration, and the final status of the execution. If failed, then the task that led to the failure is specified.\n\n" +
        "Use this notification task only in a flow that has a [Flow trigger](https://kestra.io/docs/administrator-guide/monitoring#alerting). Don't use this notification task in `errors` tasks. Instead, for `errors` tasks, use the [GoogleChatIncomingWebhook](https://kestra.io/plugins/plugin-notifications/tasks/google-chat/io.kestra.plugin.notifications.google.googlechatincomingwebhook) task."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a Google Chat notification on a failed flow execution.",
            full = true,
            code = """
                id: failure_alert
                namespace: company.team

                tasks:
                  - id: send_alert
                    type: io.kestra.plugin.googleworkspace.chat.GoogleChatExecution
                    url: "{{ secret('GOOGLE_WEBHOOK') }}" # format: https://chat.googleapis.com/v1/spaces/xzy/messages
                    text: "Google Chat Notification"
                    executionId: "{{trigger.executionId}}"

                triggers:
                  - id: failed_prod_workflows
                    type: io.kestra.plugin.core.trigger.Flow
                    conditions:
                      - type: io.kestra.plugin.core.condition.ExecutionStatus
                        in:
                          - FAILED
                          - WARNING
                      - type: io.kestra.plugin.core.condition.ExecutionNamespace
                        namespace: prod
                        prefix: true
                """
        )
    }
)
public class GoogleChatExecution extends GoogleChatTemplate implements ExecutionInterface {
    @Builder.Default
    private final Property<String> executionId = Property.ofExpression("{{ execution.id }}");
    private Property<Map<String, Object>> customFields;
    private Property<String> customMessage;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        this.templateUri = Property.ofValue("google-chat-template.peb");
        this.templateRenderMap = Property.ofValue(ExecutionService.executionMap(runContext, this));

        return super.run(runContext);
    }
}
