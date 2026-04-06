package io.kestra.plugin.googleworkspace.mail.models;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;
import io.kestra.core.models.annotations.PluginProperty;

@Data
@Builder
@Jacksonized
@Schema(title = "Gmail message representation")
public class Message {
    @Schema(title = "The immutable ID of the message")
    @PluginProperty(group = "advanced")
    private String id;

    @Schema(title = "The ID of the thread the message belongs to")
    @PluginProperty(group = "advanced")
    private String threadId;

    @Schema(title = "List of IDs of labels applied to this message")
    @PluginProperty(group = "advanced")
    private List<String> labelIds;

    @Schema(title = "A short part of the message text")
    @PluginProperty(group = "advanced")
    private String snippet;

    @Schema(title = "The ID of the last history record that modified this message")
    @PluginProperty(group = "advanced")
    private String historyId;

    @Schema(title = "The internal message creation timestamp")
    @PluginProperty(group = "advanced")
    private Instant internalDate;

    @Schema(title = "Estimated size in bytes of the message")
    @PluginProperty(group = "advanced")
    private Long sizeEstimate;

    @Schema(title = "The entire email message in an RFC 2822 formatted and base64url encoded string")
    @PluginProperty(group = "advanced")
    private String raw;

    @Schema(title = "The parsed headers of the message")
    @PluginProperty(group = "advanced")
    private Map<String, String> headers;

    @Schema(title = "The message subject")
    @PluginProperty(group = "advanced")
    private String subject;

    @Schema(title = "The sender email address")
    @PluginProperty(group = "source")
    private String from;

    @Schema(title = "The recipient email addresses")
    @PluginProperty(group = "destination")
    private List<String> to;

    @Schema(title = "The CC recipient email addresses")
    @PluginProperty(group = "advanced")
    private List<String> cc;

    @Schema(title = "The BCC recipient email addresses")
    @PluginProperty(group = "advanced")
    private List<String> bcc;

    @Schema(title = "The plain text body of the message")
    @PluginProperty(group = "advanced")
    private String textPlain;

    @Schema(title = "The HTML body of the message")
    @PluginProperty(group = "advanced")
    private String textHtml;

    @Schema(title = "List of attachments in the message")
    @PluginProperty(group = "advanced")
    private List<Attachment> attachments;
}