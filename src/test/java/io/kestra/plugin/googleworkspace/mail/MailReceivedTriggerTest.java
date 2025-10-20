package io.kestra.plugin.googleworkspace.mail;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.googleworkspace.UtilsTest;
import io.kestra.plugin.googleworkspace.mail.models.EmailMetadata;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@DisabledIf(
    value = "isServiceAccountNotExists",
    disabledReason = "Disabled for CI/CD"
)
class MailReceivedTriggerTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private GmailTestUtils testUtils;

    private String clientId;
    private String clientSecret;
    private String refreshToken;

    @BeforeEach
    void setUp() {
        clientId = testUtils.getClientId();
        clientSecret = testUtils.getClientSecret();
        refreshToken = testUtils.getRefreshToken();

        Assumptions.assumeTrue(
            clientId != null && clientSecret != null && refreshToken != null,
            "Gmail OAuth credentials not configured"
        );
    }

    @Test
    void basicTrigger() throws Exception {
        String testMessageId = null;

        try{
            testMessageId = testUtils.sendTestEmail(
                "Test subject" + IdUtils.create(),
                "Test body content"
            );

            Thread.sleep(2000);

            MailReceivedTrigger trigger = MailReceivedTrigger.builder()
                .id("test-trigger")
                .type(MailReceivedTrigger.class.getName())
                .clientId(Property.ofValue(clientId))
                .clientSecret(Property.ofValue(clientSecret))
                .refreshToken(Property.ofValue(refreshToken))
                .interval(Duration.ofMinutes(1))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

            assertThat(execution.isPresent(), is(true));

            List<EmailMetadata> messages = (List<EmailMetadata>) execution.get()
                .getTrigger().getVariables().get("messages");

            assertThat(messages, notNullValue());
            assertThat(messages.size(), greaterThanOrEqualTo(1));

            EmailMetadata firstMessage = messages.getFirst();
            assertThat(firstMessage.getId(), notNullValue());
            assertThat(firstMessage.getSubject(), notNullValue());
            assertThat(firstMessage.getFrom(), notNullValue());

        } finally {
            if(testMessageId != null) {
                testUtils.deleteMessage(testMessageId);
            }
        }
    }

    @Test
    void noNewMessages() throws Exception {
        MailReceivedTrigger trigger = MailReceivedTrigger.builder()
            .id("test-trigger")
            .type(MailReceivedTrigger.class.getName())
            .clientId(Property.ofValue(clientId))
            .clientSecret(Property.ofValue(clientSecret))
            .refreshToken(Property.ofValue(refreshToken))
            .query(Property.ofValue("subject:NONEXISTENT_SUBJECT_" + IdUtils.create()))
            .interval(Duration.ofMinutes(1))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(false));
    }

    private static boolean isServiceAccountNotExists() {
        return UtilsTest.class
            .getClassLoader()
            .getResource(".gmail-oauth.json") == null;
    }
}
