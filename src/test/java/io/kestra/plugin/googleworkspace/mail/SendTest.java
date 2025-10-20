package io.kestra.plugin.googleworkspace.mail;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.googleworkspace.UtilsTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import java.util.List;

@KestraTest
@DisabledIf(
    value = "isServiceAccountNotExists",
    disabledReason = "Disabled for CI/CD"
)
class SendTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void sendSimpleEmail() throws Exception {
        RunContext runContext = runContextFactory.of();

        Send task = Send.builder()
            .clientId(Property.ofValue(UtilsTest.oauthClientId()))
            .clientSecret(Property.ofValue(UtilsTest.oauthClientSecret()))
            .refreshToken(Property.ofValue(UtilsTest.oauthRefreshToken()))
            .to(Property.ofValue(List.of("test@example.com")))
            .subject(Property.ofValue("Test Email from Kestra"))
            .textBody(Property.ofValue("This is a test email sent from Kestra Gmail plugin."))
            .build();

        Send.Output output = task.run(runContext);

        assertThat(output.getMessageId(), is(notNullValue()));
        assertThat(output.getThreadId(), is(notNullValue()));
    }

    @Test
    void sendHtmlEmail() throws Exception {
        RunContext runContext = runContextFactory.of();

        Send task = Send.builder()
            .clientId(Property.ofValue(UtilsTest.oauthClientId()))
            .clientSecret(Property.ofValue(UtilsTest.oauthClientSecret()))
            .refreshToken(Property.ofValue(UtilsTest.oauthRefreshToken()))
            .to(Property.ofValue(List.of("test@example.com")))
            .cc(Property.ofValue(List.of("cc@example.com")))
            .subject(Property.ofValue("HTML Test Email"))
            .htmlBody(Property.ofValue("<h1>Test Email</h1><p>This is an <b>HTML</b> email from Kestra.</p>"))
            .build();

        Send.Output output = task.run(runContext);

        assertThat(output.getMessageId(), is(notNullValue()));
        assertThat(output.getThreadId(), is(notNullValue()));
    }

    @Test
    void sendMultipartEmail() throws Exception {
        RunContext runContext = runContextFactory.of();

        Send task = Send.builder()
            .clientId(Property.ofValue(UtilsTest.oauthClientId()))
            .clientSecret(Property.ofValue(UtilsTest.oauthClientSecret()))
            .refreshToken(Property.ofValue(UtilsTest.oauthRefreshToken()))
            .to(Property.ofValue(List.of("test@example.com")))
            .subject(Property.ofValue("Multipart Test Email"))
            .textBody(Property.ofValue("This is the plain text version."))
            .htmlBody(Property.ofValue("<p>This is the <em>HTML</em> version.</p>"))
            .build();

        Send.Output output = task.run(runContext);

        assertThat(output.getMessageId(), is(notNullValue()));
        assertThat(output.getThreadId(), is(notNullValue()));
    }

    private static boolean isServiceAccountNotExists() {
        return UtilsTest.class
            .getClassLoader()
            .getResource("isServiceAccountNotExists") == null;
    }
}