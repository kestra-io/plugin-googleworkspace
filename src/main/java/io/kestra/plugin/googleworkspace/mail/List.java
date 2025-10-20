package io.kestra.plugin.googleworkspace.mail;

import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.ListMessagesResponse;
import com.google.api.services.gmail.model.Message;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
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
                    fetchType: STORE
                """
        ),
        @Example(
            title = "Get first message only",
            full = true,
            code = """
                id: get_first_message
                namespace: company.team

                tasks:
                  - id: get_first
                    type: io.kestra.plugin.googleworkspace.mail.List
                    clientId: "{{ secret('GMAIL_CLIENT_ID') }}"
                    clientSecret: "{{ secret('GMAIL_CLIENT_SECRET') }}"
                    refreshToken: "{{ secret('GMAIL_REFRESH_TOKEN') }}"
                    fetchType: FETCH_ONE
                    maxResults: 1
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

    @Schema(
        title = "The way you want to store the data",
        description = """
            FETCH - outputs the messages as an output
            FETCH_ONE - outputs the first message only as an output
            STORE - stores all messages to a file
            NONE - no output"""
    )
    @NotNull
    @Builder.Default
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

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

        // Transform Gmail messages to our message model
        java.util.List<io.kestra.plugin.googleworkspace.mail.models.Message> transformedMessages = messages.stream()
            .map(msg -> io.kestra.plugin.googleworkspace.mail.models.Message.builder()
                .id(msg.getId())
                .threadId(msg.getThreadId())
                .build())
            .toList();

        // Handle different fetch types
        var rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        Output.OutputBuilder output = Output.builder()
            .resultSizeEstimate(response.getResultSizeEstimate() != null ? response.getResultSizeEstimate().intValue() : 0)
            .nextPageToken(response.getNextPageToken());

        switch (rFetchType) {
            case FETCH_ONE -> {
                if (!transformedMessages.isEmpty()) {
                    output.message(transformedMessages.get(0));
                }
            }
            case STORE -> {
                if (!transformedMessages.isEmpty()) {
                    File tempFile = this.storeMessages(runContext, transformedMessages);
                    output.uri(runContext.storage().putFile(tempFile));
                }
            }
            case FETCH -> {
                output.messages(transformedMessages);
            }
            case NONE -> {
                // No output needed
            }
        }

        return output.build();
    }

    private File storeMessages(RunContext runContext, java.util.List<io.kestra.plugin.googleworkspace.mail.models.Message> messages) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
            Flux<io.kestra.plugin.googleworkspace.mail.models.Message> flux = Flux.fromIterable(messages);
            FileSerde.writeAll(fileWriter, flux).block();
        }

        return tempFile;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of messages (when fetchType is FETCH)")
        private java.util.List<io.kestra.plugin.googleworkspace.mail.models.Message> messages;

        @Schema(title = "Single message (when fetchType is FETCH_ONE)")
        private io.kestra.plugin.googleworkspace.mail.models.Message message;

        @Schema(title = "URI of the stored messages file (when fetchType is STORE)")
        private URI uri;

        @Schema(title = "Total estimated number of results")
        private Integer resultSizeEstimate;

        @Schema(title = "Token for retrieving next page of results")
        private String nextPageToken;
    }
}