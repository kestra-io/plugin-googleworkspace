package io.kestra.plugin.googleworkspace.mail;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.Message;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.UserCredentials;
import io.kestra.plugin.googleworkspace.UtilsTest;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.ByteArrayOutputStream;
import java.util.Base64;
import java.util.Collections;
import java.util.Properties;

@Singleton
public class GmailTestUtils {
    private static final Logger logger = LoggerFactory.getLogger(GmailTestUtils.class);

    private Gmail gmail;

    public String getClientId() {
        try {
            return UtilsTest.oauthClientId();
        } catch (Exception e) {
            logger.error("Failed to get client ID", e);
            return null;
        }
    }

    public String getClientSecret() {
        try {
            return UtilsTest.oauthClientSecret();
        } catch (Exception e) {
            logger.error("Failed to get client secret", e);
            return null;
        }
    }

    public String getRefreshToken() {
        try {
            return UtilsTest.oauthRefreshToken();
        } catch (Exception e) {
            logger.error("Failed to get refresh token", e);
            return null;
        }
    }

    public String getTestEmail() {
        try {
            // Try to get test email from the OAuth credentials file
            String email = UtilsTest.getOAuthCredential("test_email");
            if (email != null && !email.trim().isEmpty()) {
                return email;
            } else {
                logger.warn("Test email is null or empty in credentials, using default");
                return "test@example.com";
            }
        } catch (Exception e) {
            // Fall back to a default test email if not found in credentials
            logger.warn("Test email not found in credentials, using default");
            return "test@example.com";
        }
    }

    private Gmail getGmail() throws Exception {
        if (gmail == null) {
            UserCredentials credentials = UserCredentials.newBuilder()
                .setClientId(getClientId())
                .setClientSecret(getClientSecret())
                .setRefreshToken(getRefreshToken())
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
        String testEmailAddress = getTestEmail();
        email.setFrom(new InternetAddress(testEmailAddress));
        email.addRecipient(javax.mail.Message.RecipientType.TO, new InternetAddress(testEmailAddress));
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
