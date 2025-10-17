package io.kestra.plugin.googleworkspace.mail;

import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.Message;
import com.google.api.services.gmail.model.MessagePart;
import com.google.api.services.gmail.model.MessagePartHeader;
import com.google.api.client.util.Base64;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.googleworkspace.mail.models.Attachment;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import jakarta.validation.constraints.NotNull;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get a Gmail message",
    description = "Retrieve a specific Gmail message by ID with full content and metadata"
)
@Plugin(
    examples = {
        @Example(
            title = "Get a message by ID",
            full = true,
            code = """
                id: get_gmail_message
                namespace: company.team

                tasks:
                  - id: list_messages
                    type: io.kestra.plugin.googleworkspace.mail.List
                    clientId: "{{ secret('GMAIL_CLIENT_ID') }}"
                    clientSecret: "{{ secret('GMAIL_CLIENT_SECRET') }}"
                    refreshToken: "{{ secret('GMAIL_REFRESH_TOKEN') }}"
                    maxResults: 1

                  - id: get_message
                    type: io.kestra.plugin.googleworkspace.mail.Get
                    clientId: "{{ secret('GMAIL_CLIENT_ID') }}"
                    clientSecret: "{{ secret('GMAIL_CLIENT_SECRET') }}"
                    refreshToken: "{{ secret('GMAIL_REFRESH_TOKEN') }}"
                    messageId: "{{ outputs.list_messages.messages[0].id }}"
                """
        ),
        @Example(
            title = "Get message with specific format",
            full = true,
            code = """
                id: get_message_metadata
                namespace: company.team

                tasks:
                  - id: get_full_message
                    type: io.kestra.plugin.googleworkspace.mail.Get
                    clientId: "{{ secret('GMAIL_CLIENT_ID') }}"
                    clientSecret: "{{ secret('GMAIL_CLIENT_SECRET') }}"
                    refreshToken: "{{ secret('GMAIL_REFRESH_TOKEN') }}"
                    messageId: "1a2b3c4d5e6f7890"
                    format: full
                """
        )
    }
)
public class Get extends AbstractMail implements RunnableTask<Get.Output> {
    @Schema(
        title = "Message ID",
        description = "The ID of the message to retrieve"
    )
    @NotNull
    private Property<String> messageId;

    @Schema(
        title = "Message format",
        description = "The format to return the message payload in (options: minimal, full, raw, metadata)"
    )
    @Builder.Default
    private Property<String> format = Property.ofValue("full");

    @Override
    public Output run(RunContext runContext) throws Exception {
        Gmail gmail = this.connection(runContext);

        var rMessageId = runContext.render(this.messageId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("messageId is required"));
        
        var rFormat = runContext.render(this.format).as(String.class).orElse("full");

        Gmail.Users.Messages.Get request = gmail.users().messages().get("me", rMessageId);
        request.setFormat(rFormat);

        Message message = request.execute();


        return Output.builder()
            .message(convertMessage(message))
            .build();
    }

    private io.kestra.plugin.googleworkspace.mail.models.Message convertMessage(Message message) {
        var builder = io.kestra.plugin.googleworkspace.mail.models.Message.builder()
            .id(message.getId())
            .threadId(message.getThreadId())
            .labelIds(message.getLabelIds())
            .snippet(message.getSnippet())
            .historyId(message.getHistoryId() != null ? message.getHistoryId().toString() : null)
            .sizeEstimate(message.getSizeEstimate() != null ? message.getSizeEstimate().longValue() : null)
            .raw(message.getRaw());

        if (message.getInternalDate() != null) {
            builder.internalDate(Instant.ofEpochMilli(message.getInternalDate()));
        }

        MessagePart payload = message.getPayload();
        if (payload != null) {
            // Parse headers
            Map<String, String> headers = new HashMap<>();
            java.util.List<MessagePartHeader> headerList = payload.getHeaders();
            if (headerList != null) {
                for (MessagePartHeader header : headerList) {
                    headers.put(header.getName().toLowerCase(), header.getValue());
                }
                builder.headers(headers);
                
                // Extract common headers
                builder.subject(headers.get("subject"));
                builder.from(headers.get("from"));
                
                String toHeader = headers.get("to");
                if (toHeader != null) {
                    builder.to(parseEmailList(toHeader));
                }
                
                String ccHeader = headers.get("cc");
                if (ccHeader != null) {
                    builder.cc(parseEmailList(ccHeader));
                }
                
                String bccHeader = headers.get("bcc");
                if (bccHeader != null) {
                    builder.bcc(parseEmailList(bccHeader));
                }
            }

            // Parse body content and attachments
            java.util.List<Attachment> attachments = new ArrayList<>();
            String[] bodyContent = extractBodyAndAttachments(payload, attachments);
            builder.textPlain(bodyContent[0])
                   .textHtml(bodyContent[1])
                   .attachments(attachments);
        }

        return builder.build();
    }

    private java.util.List<String> parseEmailList(String emailHeader) {
        if (emailHeader == null || emailHeader.trim().isEmpty()) {
            return Collections.emptyList();
        }
        
        return Arrays.stream(emailHeader.split(","))
            .map(String::trim)
            .filter(email -> !email.isEmpty())
            .collect(Collectors.toList());
    }

    private String[] extractBodyAndAttachments(MessagePart part, java.util.List<Attachment> attachments) {
        String[] result = new String[2]; // [textPlain, textHtml]
        
        if (part.getParts() != null) {
            // Multipart message
            for (MessagePart subPart : part.getParts()) {
                String[] subResult = extractBodyAndAttachments(subPart, attachments);
                if (result[0] == null) result[0] = subResult[0];
                if (result[1] == null) result[1] = subResult[1];
            }
        } else {
            // Single part message
            String mimeType = part.getMimeType();
            if (mimeType != null) {
                if (mimeType.equals("text/plain") && part.getBody() != null && part.getBody().getData() != null) {
                    result[0] = new String(Base64.decodeBase64(part.getBody().getData()), StandardCharsets.UTF_8);
                } else if (mimeType.equals("text/html") && part.getBody() != null && part.getBody().getData() != null) {
                    result[1] = new String(Base64.decodeBase64(part.getBody().getData()), StandardCharsets.UTF_8);
                } else if (isAttachment(part)) {
                    // Handle attachment
                    String filename = getFilename(part);
                    if (filename != null) {
                        Attachment attachment = Attachment.builder()
                            .attachmentId(part.getBody() != null ? part.getBody().getAttachmentId() : null)
                            .mimeType(mimeType)
                            .filename(filename)
                            .size(part.getBody() != null ? part.getBody().getSize() : null)
                            .data(part.getBody() != null ? part.getBody().getData() : null)
                            .build();
                        attachments.add(attachment);
                    }
                }
            }
        }
        
        return result;
    }

    private boolean isAttachment(MessagePart part) {
        if (part.getFilename() != null && !part.getFilename().isEmpty()) {
            return true;
        }
        
        java.util.List<MessagePartHeader> headers = part.getHeaders();
        if (headers != null) {
            for (MessagePartHeader header : headers) {
                if ("Content-Disposition".equalsIgnoreCase(header.getName()) && 
                    header.getValue() != null && header.getValue().contains("attachment")) {
                    return true;
                }
            }
        }
        
        return false;
    }

    private String getFilename(MessagePart part) {
        if (part.getFilename() != null && !part.getFilename().isEmpty()) {
            return part.getFilename();
        }
        
        java.util.List<MessagePartHeader> headers = part.getHeaders();
        if (headers != null) {
            for (MessagePartHeader header : headers) {
                if ("Content-Disposition".equalsIgnoreCase(header.getName()) && header.getValue() != null) {
                    String value = header.getValue();
                    int filenameIndex = value.indexOf("filename=");
                    if (filenameIndex != -1) {
                        String filename = value.substring(filenameIndex + 9);
                        if (filename.startsWith("\"") && filename.endsWith("\"")) {
                            filename = filename.substring(1, filename.length() - 1);
                        }
                        return filename;
                    }
                }
            }
        }
        
        return null;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The retrieved message")
        private io.kestra.plugin.googleworkspace.mail.models.Message message;
    }
}