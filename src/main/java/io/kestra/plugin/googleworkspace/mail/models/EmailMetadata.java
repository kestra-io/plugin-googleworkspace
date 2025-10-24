package io.kestra.plugin.googleworkspace.mail.models;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Builder
@Getter
@Jacksonized
public class EmailMetadata {
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
