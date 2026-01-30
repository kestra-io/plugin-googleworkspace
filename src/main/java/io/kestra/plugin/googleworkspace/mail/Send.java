package io.kestra.plugin.googleworkspace.mail;

import com.google.api.client.util.Base64;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.Message;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import jakarta.validation.constraints.NotNull;
import lombok.experimental.SuperBuilder;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.internet.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Send an email with Gmail API",
    description = "Sends a message using OAuth Gmail access. Supports text/HTML bodies, CC/BCC, and attachments from kestra:// URIs."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a simple text email",
            full = true,
            code = """
                id: send_simple_email
                namespace: company.team

                tasks:
                  - id: send_email
                    type: io.kestra.plugin.googleworkspace.mail.Send
                    clientId: "{{ secret('GMAIL_CLIENT_ID') }}"
                    clientSecret: "{{ secret('GMAIL_CLIENT_SECRET') }}"
                    refreshToken: "{{ secret('GMAIL_REFRESH_TOKEN') }}"
                    to:
                      - recipient@example.com
                    subject: Test Email
                    textBody: This is a test email from Kestra
                """
        ),
        @Example(
            title = "Send an HTML email with CC and attachments",
            full = true,
            code = """
                id: send_rich_email
                namespace: company.team

                tasks:
                  - id: send_rich_email
                    type: io.kestra.plugin.googleworkspace.mail.Send
                    clientId: "{{ secret('GMAIL_CLIENT_ID') }}"
                    clientSecret: "{{ secret('GMAIL_CLIENT_SECRET') }}"
                    refreshToken: "{{ secret('GMAIL_REFRESH_TOKEN') }}"
                    to:
                      - recipient@example.com
                    cc:
                      - cc1@example.com
                      - cc2@example.com
                    subject: Rich Email
                    htmlBody: "<h1>Hello</h1><p>This is a <b>rich</b> email!</p>"
                    attachments:
                      - /path/to/file.pdf
                      - /path/to/image.png
                """
        )
    }
)
public class Send extends AbstractMail implements RunnableTask<Send.Output> {
    @Schema(
        title = "To recipients",
        description = "Primary recipient email addresses"
    )
    @NotNull
    private Property<List<String>> to;

    @Schema(
        title = "CC recipients",
        description = "Carbon copy addresses"
    )
    private Property<List<String>> cc;

    @Schema(
        title = "BCC recipients",
        description = "Blind carbon copy addresses"
    )
    private Property<List<String>> bcc;

    @Schema(
        title = "Subject",
        description = "Subject line"
    )
    private Property<String> subject;

    @Schema(
        title = "Plain text body",
        description = "Plain text content"
    )
    private Property<String> textBody;

    @Schema(
        title = "HTML body",
        description = "HTML content"
    )
    private Property<String> htmlBody;

    @Schema(
        title = "From address",
        description = "Sender email; defaults to authenticated Gmail user"
    )
    private Property<String> from;

    @Schema(
        title = "Attachments",
        description = "List of kestra:// file URIs to attach"
    )
    private Property<List<String>> attachments;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Gmail gmail = this.connection(runContext);

        // Create the email message using javax.mail (following Google's official docs)
        MimeMessage email = createEmail(runContext);

        // Encode and send the message
        Message message = createMessageWithEmail(email);
        message = gmail.users().messages().send("me", message).execute();

        runContext.logger().info("Email sent successfully with message ID: {}", message.getId());

        return Output.builder()
            .messageId(message.getId())
            .threadId(message.getThreadId())
            .build();
    }

    private MimeMessage createEmail(RunContext runContext) throws Exception {
        Properties props = new Properties();
        Session session = Session.getDefaultInstance(props, null);
        MimeMessage email = new MimeMessage(session);

        // Set recipients
        List<String> rToAddresses = runContext.render(this.to).asList(String.class);
        if (rToAddresses != null && !rToAddresses.isEmpty()) {
            runContext.logger().debug("Setting TO recipients: {}", rToAddresses.size());
            InternetAddress[] toArray = rToAddresses.stream()
                .map(addr -> {
                    try {
                        return new InternetAddress(addr);
                    } catch (AddressException e) {
                        throw new RuntimeException("Invalid email address: " + addr, e);
                    }
                })
                .toArray(InternetAddress[]::new);
            email.setRecipients(javax.mail.Message.RecipientType.TO, toArray);
        }

        // Set CC recipients
        var rCcAddresses = runContext.render(this.cc).asList(String.class);
        if (rCcAddresses != null && !rCcAddresses.isEmpty()) {
            runContext.logger().debug("Setting CC recipients: {}", rCcAddresses.size());
            InternetAddress[] ccArray = rCcAddresses.stream()
                .map(addr -> {
                    try {
                        return new InternetAddress(addr);
                    } catch (AddressException e) {
                        throw new RuntimeException("Invalid CC email address: " + addr, e);
                    }
                })
                .toArray(InternetAddress[]::new);
            email.setRecipients(javax.mail.Message.RecipientType.CC, ccArray);
        }

        // Set BCC recipients
        var rBccAddresses = runContext.render(this.bcc).asList(String.class);
        if (rBccAddresses != null && !rBccAddresses.isEmpty()) {
            runContext.logger().debug("Setting BCC recipients: {}", rBccAddresses.size());
            InternetAddress[] bccArray = rBccAddresses.stream()
                .map(addr -> {
                    try {
                        return new InternetAddress(addr);
                    } catch (AddressException e) {
                        throw new RuntimeException("Invalid BCC email address: " + addr, e);
                    }
                })
                .toArray(InternetAddress[]::new);
            email.setRecipients(javax.mail.Message.RecipientType.BCC, bccArray);
        }

        // Set from address
        var rFromAddress = runContext.render(this.from).as(String.class).orElse(null);
        if (rFromAddress != null && !rFromAddress.isEmpty()) {
            try {
                email.setFrom(new InternetAddress(rFromAddress));
                runContext.logger().debug("Set from address: {}", rFromAddress);
            } catch (AddressException e) {
                // If from address is invalid, let Gmail use the authenticated user's address
            }
        }

        // Set subject
        var rSubject = runContext.render(this.subject).as(String.class).orElse("");
        email.setSubject(rSubject);
        runContext.logger().debug("Set email subject: {}", rSubject);

        // Handle content and attachments
        var rAttachmentList = runContext.render(this.attachments).asList(String.class);
        if (rAttachmentList != null && !rAttachmentList.isEmpty()) {
            runContext.logger().debug("Adding {} attachments to email", rAttachmentList.size());
            // Create multipart message with attachments
            Multipart multipart = new MimeMultipart();

            // Add body content
            MimeBodyPart bodyPart = new MimeBodyPart();
            setBodyContent(runContext, bodyPart);
            multipart.addBodyPart(bodyPart);

            // Add attachments
            for (String attachmentUri : rAttachmentList) {
                MimeBodyPart attachmentPart = new MimeBodyPart();
                URI uri = URI.create(attachmentUri);
                DataSource source = new FileDataSource(runContext.workingDir().path().resolve(uri.getPath()).toFile());
                attachmentPart.setDataHandler(new DataHandler(source));
                attachmentPart.setFileName(source.getName());
                multipart.addBodyPart(attachmentPart);
            }

            email.setContent(multipart);
        } else {
            // No attachments, set body directly
            setBodyContent(runContext, email);
        }

        return email;
    }

    private void setBodyContent(RunContext runContext, MimePart part) throws Exception {
        var rTextContent = runContext.render(this.textBody).as(String.class).orElse(null);
        var rHtmlContent = runContext.render(this.htmlBody).as(String.class).orElse(null);

        if (rHtmlContent != null && rTextContent != null) {
            // Both text and HTML - create multipart alternative
            Multipart multipart = new MimeMultipart("alternative");
            
            MimeBodyPart textPart = new MimeBodyPart();
            textPart.setText(rTextContent, "utf-8");
            multipart.addBodyPart(textPart);
            
            MimeBodyPart htmlPart = new MimeBodyPart();
            htmlPart.setContent(rHtmlContent, "text/html; charset=utf-8");
            multipart.addBodyPart(htmlPart);
            
            part.setContent(multipart);
        } else if (rHtmlContent != null) {
            // HTML only
            part.setContent(rHtmlContent, "text/html; charset=utf-8");
        } else if (rTextContent != null) {
            // Text only
            part.setText(rTextContent, "utf-8");
        } else {
            // No content
            part.setText("", "utf-8");
        }
    }

    private Message createMessageWithEmail(MimeMessage email) throws MessagingException, IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        email.writeTo(buffer);
        byte[] bytes = buffer.toByteArray();
        String encodedEmail = Base64.encodeBase64URLSafeString(bytes);

        Message message = new Message();
        message.setRaw(encodedEmail);
        return message;
    }


    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Sent message ID")
        private String messageId;

        @Schema(title = "Thread ID of sent message")
        private String threadId;
    }
}
