package io.kestra.plugin.googleworkspace.mail.models;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Builder
@Jacksonized
@Schema(title = "Gmail message attachment")
public class Attachment {
    @Schema(title = "The attachment ID")
    private String attachmentId;

    @Schema(title = "The MIME type of the attachment file")
    private String mimeType;

    @Schema(title = "The filename of the attachment")
    private String filename;

    @Schema(title = "The size of the attachment in bytes")
    private Integer size;

    @Schema(title = "The attachment data as a base64url encoded string")
    private String data;
}