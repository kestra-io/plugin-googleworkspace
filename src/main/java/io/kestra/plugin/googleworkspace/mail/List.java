package io.kestra.plugin.googleworkspace.mail;

import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.ListMessagesResponse;
import com.google.api.services.gmail.model.Message;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List Gmail messages",
    description = "Retrieve a list of messages from Gmail inbox or specific labels using Gmail API"
)
@Plugin(
    examples = {
        @Example(
            title = "List all messages in inbox",
            full = true,
            code = """
                id: list_messages
                namespace: company.team

                tasks:
                  - id: list_messages
                    type: io.kestra.plugin.googleworkspace.mail.List
                    clientId: "{{ secret('GMAIL_CLIENT_ID') }}"
                    clientSecret: "{{ secret('GMAIL_CLIENT_SECRET') }}"
                    refreshToken: "{{ secret('GMAIL_REFRESH_TOKEN') }}"
                    maxResults: 10
                """
        ),
        @Example(
            title = "List unread messages",
            full = true,
            code = """
                id: list_unread_messages
                namespace: company.team

                tasks:
                  - id: list_unread
                    type: io.kestra.plugin.googleworkspace.mail.List
                    clientId: "{{ secret('GMAIL_CLIENT_ID') }}"
                    clientSecret: "{{ secret('GMAIL_CLIENT_SECRET') }}"
                    refreshToken: "{{ secret('GMAIL_REFRESH_TOKEN') }}"
                    query: is:unread
                    labelIds:
                      - INBOX
                    maxResults: 50
                """
        )
    }
)
public class List extends AbstractMail implements RunnableTask<List.Output> {
    @Schema(
        title = "Gmail search query",
        description = "Search query using Gmail search syntax (e.g., 'is:unread', 'from:sender@example.com', 'subject:important')"
    )
    private Property<String> query;

    @Schema(
        title = "Label IDs to filter messages",
        description = "List of label IDs to restrict the search (e.g., INBOX, SENT, DRAFT, UNREAD)"
    )
    private Property<java.util.List<String>> labelIds;

    @Schema(
        title = "Maximum number of results",
        description = "Maximum number of messages to return (default: 100, max: 500)"
    )
    @Builder.Default
    private Property<Integer> maxResults = Property.ofValue(100);

    @Schema(
        title = "Include spam and trash",
        description = "Whether to include messages from SPAM and TRASH in the results"
    )
    @Builder.Default
    private Property<Boolean> includeSpamTrash = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Gmail gmail = this.connection(runContext);

        Gmail.Users.Messages.List request = gmail.users().messages().list("me");

        // Set query parameters
        var rQuery = runContext.render(this.query).as(String.class).orElse(null);
        if (rQuery != null && !rQuery.trim().isEmpty()) {
            request.setQ(rQuery);
        }

        var rLabelIds = runContext.render(this.labelIds).asList(String.class);
        if (rLabelIds != null && !rLabelIds.isEmpty()) {
            request.setLabelIds(rLabelIds);
        }

        var rMaxResults = runContext.render(this.maxResults).as(Integer.class).orElse(100);
        request.setMaxResults((long) Math.min(rMaxResults, 500)); // Gmail API limit is 500

        var rIncludeSpamTrash = runContext.render(this.includeSpamTrash).as(Boolean.class).orElse(false);
        request.setIncludeSpamTrash(rIncludeSpamTrash);

        // Execute the request
        ListMessagesResponse response = request.execute();

        java.util.List<Message> messages = response.getMessages();
        if (messages == null) {
            messages = new ArrayList<>();
        }


        return Output.builder()
            .messages(messages.stream()
                .map(msg -> io.kestra.plugin.googleworkspace.mail.models.Message.builder()
                    .id(msg.getId())
                    .threadId(msg.getThreadId())
                    .build())
                .toList())
            .resultSizeEstimate(response.getResultSizeEstimate() != null ? response.getResultSizeEstimate().intValue() : 0)
            .nextPageToken(response.getNextPageToken())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of messages")
        private java.util.List<io.kestra.plugin.googleworkspace.mail.models.Message> messages;

        @Schema(title = "Total estimated number of results")
        private Integer resultSizeEstimate;

        @Schema(title = "Token for retrieving next page of results")
        private String nextPageToken;
    }
}