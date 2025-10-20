package io.kestra.plugin.googleworkspace.mail;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.Message;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.UserCredentials;
import io.micronaut.context.annotation.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.ByteArrayOutputStream;
import java.util.Base64;
import java.util.Collections;
import java.util.Properties;


public class GmailTestUtils {
    private static final Logger logger = LoggerFactory.getLogger(GmailTestUtils.class);

    @Value("${kestra.tasks.googleworkspace.mail.client-id:}")
    private String clientId;

    @Value("${kestra.tasks.googleworkspace.mail.client-secret:}")
    private String clientSecret;

    @Value("${kestra.tasks.googleworkspace.mail.refresh-token:}")
    private String refreshToken;

    @Value("${kestra.tasks.googleworkspace.mail.test-email:}")
    private String testEmail;

    private Gmail gmail;

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    private Gmail getGmail() throws Exception {
        if (gmail == null) {
            UserCredentials credentials = UserCredentials.newBuilder()
                .setClientId(clientId)
                .setClientSecret(clientSecret)
                .setRefreshToken(refreshToken)
                .build();

            GoogleCredentials scopedCredentials = credentials.createScoped(
                Collections.singleton("https://www.googleapis.com/auth/gmail.modify")
            );

            gmail = new Gmail.Builder(
                GoogleNetHttpTransport.newTrustedTransport(),
                GsonFactory.getDefaultInstance(),
                new HttpCredentialsAdapter(scopedCredentials)
            )
                .setApplicationName("kestra test")
                .build();
        }
        return gmail;
    }

    public String sendTestEmail(String subject, String body) throws Exception {
        Gmail gmail = getGmail();

        Properties props = new Properties();
        Session session = Session.getDefaultInstance(props, null);

        MimeMessage email = new MimeMessage(session);
        email.setFrom(new InternetAddress(testEmail));
        email.addRecipient(javax.mail.Message.RecipientType.TO, new InternetAddress(testEmail));
        email.setSubject(subject);
        email.setText(body);

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        email.writeTo(buffer);
        byte[] bytes = buffer.toByteArray();
        String encodedEmail = Base64.getUrlEncoder().encodeToString(bytes);

        Message message = new Message();
        message.setRaw(encodedEmail);

        Message sent = gmail.users().messages().send("me", message).execute();
        logger.debug("Sent test email: {} ({})", subject, sent.getId());

        return sent.getId();
    }

    public void deleteMessage(String messageId) {
        try {
            Gmail gmail = getGmail();
            gmail.users().messages().delete("me", messageId).execute();
            logger.debug("Deleted message: {}", messageId);
        } catch (Exception e) {
            logger.warn("Failed to delete message {}: {}", messageId, e.getMessage());
        }
    }

}
