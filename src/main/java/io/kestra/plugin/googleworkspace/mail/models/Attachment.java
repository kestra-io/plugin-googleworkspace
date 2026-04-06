package io.kestra.plugin.googleworkspace.mail.models;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;
import io.kestra.core.models.annotations.PluginProperty;

@Data
@Builder
@Jacksonized
@Schema(title = "Gmail message attachment")
public class Attachment {
    @Schema(title = "The attachment ID")
    @PluginProperty(group = "advanced")
    private String attachmentId;

    @Schema(title = "The MIME type of the attachment file")
    @PluginProperty(group = "advanced")
    private String mimeType;

    @Schema(title = "The filename of the attachment")
    @PluginProperty(group = "advanced")
    private String filename;

    @Schema(title = "The size of the attachment in bytes")
    @PluginProperty(group = "advanced")
    private Integer size;

    @Schema(title = "The attachment data as a base64url encoded string")
    @PluginProperty(group = "advanced")
    private String data;
}