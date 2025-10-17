package io.kestra.plugin.googleworkspace.mail;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.ListMessagesResponse;
import com.google.api.services.gmail.model.Message;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.UserCredentials;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.googleworkspace.OAuthInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger that listens for new emails in Gmail",
    description = "Monitors Gmail inbox or specific labels for new messages and emits an event for each new email detected. " +
        "The trigger uses polling to check for new emails and filters by message timestamp to avoid duplicates."
)
@Plugin(
    examples = {
        @Example(
            title = "Monitor inbox for any new email",
            full = true,
            code = """
                id: gmail_new_messages
                namespace: company.team

                tasks:
                  - id: process_email
                    type: io.kestra.plugin.core.log.Log
                    message: "New email from {{ trigger.from }}: {{ trigger.subject }}"

                triggers:
                  - id: watch_inbox
                    type: io.kestra.plugin.googleworkspace.mail.MailReceived
                    clientId: "{{ secret('GMAIL_CLIENT_ID') }}"
                    clientSecret: "{{ secret('GMAIL_CLIENT_SECRET') }}"
                    refreshToken: "{{ secret('GMAIL_REFRESH_TOKEN') }}"
                    interval: PT5M
                """
        ),
        @Example(
            title = "Monitor specific labels with query filter",
            full = true,
            code = """
                id: gmail_urgent_messages
                namespace: company.team

                tasks:
                  - id: notify_urgent
                    type: io.kestra.plugin.notifications.slack.SlackIncomingWebhook
                    url: "{{ secret('SLACK_WEBHOOK') }}"
                    payload: |
                      {
                        "text": "Urgent email: {{ trigger.subject }} from {{ trigger.from }}"
                      }

                triggers:
                  - id: watch_urgent
                    type: io.kestra.plugin.googleworkspace.mail.MailReceived
                    clientId: "{{ secret('GMAIL_CLIENT_ID') }}"
                    clientSecret: "{{ secret('GMAIL_CLIENT_SECRET') }}"
                    refreshToken: "{{ secret('GMAIL_REFRESH_TOKEN') }}"
                    query: "is:unread label:urgent"
                    labelIds:
                      - INBOX
                      - URGENT
                    interval: PT2M
                """
        )
    }
)
public class MailReceived extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<MailReceived.Output>, OAuthInterface {

    @Schema(
        title = "OAuth 2.0 Client ID",
        description = "The OAuth 2.0 client ID from Google Cloud Console"
    )
    @NotNull
    protected Property<String> clientId;

    @Schema(
        title = "OAuth 2.0 Client Secret",
        description = "The OAuth 2.0 client secret from Google Cloud Console"
    )
    @NotNull
    protected Property<String> clientSecret;

    @Schema(
        title = "OAuth 2.0 Refresh Token",
        description = "The OAuth 2.0 refresh token obtained through the authorization flow"
    )
    @NotNull
    protected Property<String> refreshToken;

    @Schema(
        title = "OAuth 2.0 Access Token",
        description = "The OAuth 2.0 access token (optional, will be generated from refresh token if not provided)"
    )
    protected Property<String> accessToken;

    @Builder.Default
    protected Property<List<String>> scopes = Property.ofValue(List.of(
        "https://www.googleapis.com/auth/gmail.modify",
        "https://www.googleapis.com/auth/gmail.readonly",
        "https://www.googleapis.com/auth/gmail.send"
    ));

    @Builder.Default
    protected Property<Integer> readTimeout = Property.ofValue(120);

    @Schema(
        title = "Gmail search query",
        description = "Search query using Gmail search syntax (e.g., 'is:unread', 'from:sender@example.com', 'subject:important'). " +
            "If not specified, monitors all messages."
    )
    private Property<String> query;

    @Schema(
        title = "Label IDs to filter messages",
        description = "List of label IDs to restrict the search (e.g., INBOX, SENT, DRAFT, UNREAD). " +
            "If not specified, searches all accessible messages."
    )
    private Property<List<String>> labelIds;

    @Schema(
        title = "Include spam and trash",
        description = "Whether to include messages from SPAM and TRASH in the results"
    )
    @Builder.Default
    private Property<Boolean> includeSpamTrash = Property.ofValue(false);

    @Schema(
        title = "The polling interval",
        description = "How frequently to check for new emails. Must be at least PT1M (1 minute)."
    )
    @Builder.Default
    protected Duration interval = Duration.ofMinutes(5);

    @Schema(
        title = "Maximum number of messages to process per poll",
        description = "Limits the number of new messages processed in a single poll to avoid overwhelming the system"
    )
    @Builder.Default
    protected Property<Integer> maxMessagesPerPoll = Property.ofValue(50);

    @Schema(
        title = "Lookback window for first run",
        description = "On first execution, how far back to look for messages. Defaults to the polling interval."
    )
    private Property<Duration> initialLookback;

    @Override
    public Duration getInterval() {
        return this.interval;
    }

    protected Gmail connection(RunContext runContext) throws Exception {
        JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
        HttpCredentialsAdapter credentials = this.oauthCredentials(runContext);

        return new Gmail.Builder(this.netHttpTransport(), JSON_FACTORY, credentials)
            .setApplicationName("Kestra")
            .build();
    }

    protected HttpCredentialsAdapter oauthCredentials(RunContext runContext) throws Exception {
        String clientId = runContext.render(this.clientId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("clientId is required for OAuth authentication"));
        String clientSecret = runContext.render(this.clientSecret).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("clientSecret is required for OAuth authentication"));
        String refreshToken = runContext.render(this.refreshToken).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("refreshToken is required for OAuth authentication"));

        String accessToken = runContext.render(this.accessToken).as(String.class).orElse(null);

        UserCredentials.Builder credentialsBuilder = UserCredentials.newBuilder()
            .setClientId(clientId)
            .setClientSecret(clientSecret)
            .setRefreshToken(refreshToken);

        if (accessToken != null && !accessToken.trim().isEmpty()) {
            credentialsBuilder.setAccessToken(new AccessToken(accessToken, null));
        }

        GoogleCredentials credentials = credentialsBuilder.build();

        var rScopes = runContext.render(this.scopes).asList(String.class);
        if (rScopes != null && !rScopes.isEmpty()) {
            credentials = credentials.createScoped(rScopes);
        }

        var rTimeout = runContext.render(this.readTimeout).as(Integer.class).orElse(120);
        return new HttpCredentialsAdapter(credentials) {
            @Override
            public void initialize(HttpRequest request) throws java.io.IOException {
                super.initialize(request);
                request.setReadTimeout(rTimeout * 1000);
            }
        };
    }

    protected NetHttpTransport netHttpTransport() throws Exception {
        return GoogleNetHttpTransport.newTrustedTransport();
    }

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Gmail gmail = this.connection(runContext);

        // Calculate the cutoff time for new messages
        // Use last execution time, or lookback window for first run
        Instant cutoffTime = calculateCutoffTime(runContext, context, logger);

        logger.debug("Checking for messages received after: {}", cutoffTime);

        // Fetch messages (with broad date filter to minimize API calls)
        List<Message> candidateMessages = fetchMessages(gmail, runContext, cutoffTime, logger);

        if (candidateMessages.isEmpty()) {
            logger.debug("No candidate messages found");
            return Optional.empty();
        }

        logger.debug("Found {} candidate message(s), filtering by timestamp", candidateMessages.size());

        // Filter messages by precise timestamp and fetch full details
        List<Output.EmailMetadata> newMessages = filterAndEnrichMessages(
            gmail, candidateMessages, cutoffTime, runContext, logger
        );

        if (newMessages.isEmpty()) {
            logger.debug("No new messages after timestamp filtering");
            return Optional.empty();
        }

        logger.info("Found {} new message(s) after {}", newMessages.size(), cutoffTime);

        Map<String, Object> triggerVars = new HashMap<>();

        // Add the full messages list
        triggerVars.put("messages", newMessages);
        
        if (!newMessages.isEmpty()) {
            Output.EmailMetadata firstMessage = newMessages.get(0);
            triggerVars.put("id", firstMessage.getId());
            triggerVars.put("threadId", firstMessage.getThreadId());
            triggerVars.put("subject", firstMessage.getSubject());
            triggerVars.put("from", firstMessage.getFrom());
            triggerVars.put("to", firstMessage.getTo());
            triggerVars.put("cc", firstMessage.getCc());
            triggerVars.put("bcc", firstMessage.getBcc());
            triggerVars.put("snippet", firstMessage.getSnippet());
            triggerVars.put("internalDate", firstMessage.getInternalDate());
            triggerVars.put("headers", firstMessage.getHeaders());
            triggerVars.put("labelIds", firstMessage.getLabelIds());
            triggerVars.put("historyId", firstMessage.getHistoryId());
            triggerVars.put("sizeEstimate", firstMessage.getSizeEstimate());
        }

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, triggerVars);

        return Optional.of(execution);
    }

    private Instant calculateCutoffTime(RunContext runContext, TriggerContext context, Logger logger) throws Exception {
        ZonedDateTime lastExecution = context.getNextExecutionDate();

        if (lastExecution != null) {
            // Use the last scheduled execution time minus interval as cutoff
            // This ensures we don't miss messages between polls
            Instant cutoff = lastExecution.toInstant().minus(this.interval);
            logger.debug("Using last execution time: {}", lastExecution);
            return cutoff;
        } else {
            // First run: use configurable lookback or default to interval
            Duration lookback = runContext.render(this.initialLookback).as(Duration.class)
                .orElse(this.interval);
            Instant cutoff = Instant.now().minus(lookback);
            logger.debug("First run, looking back: {}", lookback);
            return cutoff;
        }
    }

    private List<Message> fetchMessages(Gmail gmail, RunContext runContext, Instant cutoffTime, Logger logger)
        throws Exception {
        List<Message> allMessages = new ArrayList<>();
        String pageToken = null;
        int maxMessages = runContext.render(this.maxMessagesPerPoll).as(Integer.class).orElse(50);

        // Use broader date range to ensure we catch all relevant messages
        // We'll filter precisely by timestamp afterwards
        String searchQuery = buildSearchQuery(runContext, cutoffTime);
        logger.debug("Gmail search query: {}", searchQuery);

        do {
            Gmail.Users.Messages.List request = gmail.users().messages().list("me");

            if (searchQuery != null && !searchQuery.trim().isEmpty()) {
                request.setQ(searchQuery);
            }

            var rLabelIds = runContext.render(this.labelIds).asList(String.class);
            if (rLabelIds != null && !rLabelIds.isEmpty()) {
                request.setLabelIds(rLabelIds);
            }

            var rIncludeSpamTrash = runContext.render(this.includeSpamTrash).as(Boolean.class).orElse(false);
            request.setIncludeSpamTrash(rIncludeSpamTrash);

            request.setMaxResults(100L);
            if (pageToken != null) {
                request.setPageToken(pageToken);
            }

            ListMessagesResponse response = request.execute();
            List<Message> messages = response.getMessages();

            if (messages != null) {
                allMessages.addAll(messages);
            }

            pageToken = response.getNextPageToken();

            // Stop if we've fetched enough for potential filtering
            if (allMessages.size() >= maxMessages * 2) {
                break;
            }

        } while (pageToken != null);

        return allMessages;
    }

    private List<Output.EmailMetadata> filterAndEnrichMessages(Gmail gmail, List<Message> messages,
                                                               Instant cutoffTime, RunContext runContext, Logger logger) throws Exception {
        List<Output.EmailMetadata> newMessages = new ArrayList<>();
        int maxMessages = runContext.render(this.maxMessagesPerPoll).as(Integer.class).orElse(50);

        for (Message message : messages) {
            if (newMessages.size() >= maxMessages) {
                logger.debug("Reached maxMessagesPerPoll limit of {}", maxMessages);
                break;
            }

            try {
                // First, get minimal metadata to check timestamp
                Message minimal = gmail.users().messages().get("me", message.getId())
                    .setFormat("minimal")
                    .execute();

                if (minimal.getInternalDate() == null) {
                    logger.warn("Message {} has no internalDate, skipping", message.getId());
                    continue;
                }

                Instant messageTime = Instant.ofEpochMilli(minimal.getInternalDate());

                // Filter by precise timestamp
                if (messageTime.isAfter(cutoffTime)) {
                    // Fetch full message details
                    Message fullMessage = gmail.users().messages().get("me", message.getId())
                        .setFormat("full")
                        .execute();

                    newMessages.add(convertToEmailMetadata(fullMessage));
                    logger.debug("Included message {} from {}", message.getId(), messageTime);
                } else {
                    logger.debug("Skipped message {} (too old: {})", message.getId(), messageTime);
                }
            } catch (Exception e) {
                logger.warn("Error processing message {}: {}", message.getId(), e.getMessage());
            }
        }

        return newMessages;
    }

    private String buildSearchQuery(RunContext runContext, Instant cutoffTime) throws Exception {
        List<String> queryParts = new ArrayList<>();

        // Add user-specified query if provided
        var rQuery = runContext.render(this.query).as(String.class).orElse(null);
        if (rQuery != null && !rQuery.trim().isEmpty()) {
            queryParts.add(rQuery);
        }

        // Add broad date filter (day before cutoff) to reduce API results
        // We'll do precise timestamp filtering afterwards
        Instant dayBeforeCutoff = cutoffTime.minus(Duration.ofDays(1));
        String afterDate = dayBeforeCutoff.toString().substring(0, 10).replace("-", "/");
        queryParts.add("after:" + afterDate);

        return queryParts.isEmpty() ? null : String.join(" ", queryParts);
    }

    private Output.EmailMetadata convertToEmailMetadata(Message message) {
        Output.EmailMetadata.EmailMetadataBuilder builder = Output.EmailMetadata.builder()
            .id(message.getId())
            .threadId(message.getThreadId())
            .labelIds(message.getLabelIds())
            .snippet(message.getSnippet())
            .historyId(message.getHistoryId() != null ? message.getHistoryId().toString() : null)
            .sizeEstimate(message.getSizeEstimate() != null ? message.getSizeEstimate().longValue() : null)
            .internalDate(message.getInternalDate() != null ? Instant.ofEpochMilli(message.getInternalDate()) : null);

        if (message.getPayload() != null && message.getPayload().getHeaders() != null) {
            Map<String, String> headers = new HashMap<>();

            message.getPayload().getHeaders().forEach(header ->
                headers.put(header.getName().toLowerCase(), header.getValue())
            );

            builder.headers(headers)
                .subject(headers.get("subject"))
                .from(headers.get("from"))
                .to(parseEmailList(headers.get("to")))
                .cc(parseEmailList(headers.get("cc")))
                .bcc(parseEmailList(headers.get("bcc")));
        }

        return builder.build();
    }

    private List<String> parseEmailList(String emailHeader) {
        if (emailHeader == null || emailHeader.trim().isEmpty()) {
            return Collections.emptyList();
        }

        return Arrays.stream(emailHeader.split(","))
            .map(String::trim)
            .filter(email -> !email.isEmpty())
            .toList();
    }

    @Builder
    @Getter
    @Jacksonized
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List of new messages found during the poll",
            description = "Each message can be accessed via trigger.messages[0], trigger.messages[1], etc. " +
                "For convenience, if only one message is found, its fields are also available directly as trigger.subject, trigger.from, etc."
        )
        private List<EmailMetadata> messages;

        @Builder
        @Getter
        @Jacksonized
        public static class EmailMetadata {
            @Schema(title = "The immutable ID of the message")
            private String id;

            @Schema(title = "The ID of the thread the message belongs to")
            private String threadId;

            @Schema(title = "List of IDs of labels applied to this message")
            private List<String> labelIds;

            @Schema(title = "A short part of the message text")
            private String snippet;

            @Schema(title = "The ID of the last history record that modified this message")
            private String historyId;

            @Schema(title = "The internal message creation timestamp")
            private Instant internalDate;

            @Schema(title = "Estimated size in bytes of the message")
            private Long sizeEstimate;

            @Schema(title = "The parsed headers of the message")
            private Map<String, String> headers;

            @Schema(title = "The message subject")
            private String subject;

            @Schema(title = "The sender email address")
            private String from;

            @Schema(title = "The recipient email addresses")
            private List<String> to;

            @Schema(title = "The CC recipient email addresses")
            private List<String> cc;

            @Schema(title = "The BCC recipient email addresses")
            private List<String> bcc;
        }
    }
}