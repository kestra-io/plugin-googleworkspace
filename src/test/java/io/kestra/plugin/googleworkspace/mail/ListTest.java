package io.kestra.plugin.googleworkspace.mail;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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

@KestraTest
@DisabledIf(
    value = "isServiceAccountNotExists",
    disabledReason = "Disabled for CI/CD"
)
class ListTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void listMessages() throws Exception {
        RunContext runContext = runContextFactory.of();

        List task = List.builder()
            .clientId(Property.ofValue(UtilsTest.oauthClientId()))
            .clientSecret(Property.ofValue(UtilsTest.oauthClientSecret()))
            .refreshToken(Property.ofValue(UtilsTest.oauthRefreshToken()))
            .maxResults(Property.ofValue(10))
            .build();

        List.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getMessages(), is(notNullValue()));
        assertThat(output.getResultSizeEstimate(), is(greaterThanOrEqualTo(0)));
    }

    @Test
    void listUnreadMessages() throws Exception {
        RunContext runContext = runContextFactory.of();

        List task = List.builder()
            .clientId(Property.ofValue(UtilsTest.oauthClientId()))
            .clientSecret(Property.ofValue(UtilsTest.oauthClientSecret()))
            .refreshToken(Property.ofValue(UtilsTest.oauthRefreshToken()))
            .query(Property.ofValue("is:unread"))
            .maxResults(Property.ofValue(5))
            .build();

        List.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getMessages(), is(notNullValue()));
        assertThat(output.getResultSizeEstimate(), is(greaterThanOrEqualTo(0)));
    }

    @Test
    void listMessagesWithLabels() throws Exception {
        RunContext runContext = runContextFactory.of();

        List task = List.builder()
            .clientId(Property.ofValue(UtilsTest.oauthClientId()))
            .clientSecret(Property.ofValue(UtilsTest.oauthClientSecret()))
            .refreshToken(Property.ofValue(UtilsTest.oauthRefreshToken()))
            .labelIds(Property.ofValue(java.util.List.of("INBOX")))
            .maxResults(Property.ofValue(15))
            .build();

        List.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getMessages(), is(notNullValue()));
        assertThat(output.getResultSizeEstimate(), is(greaterThanOrEqualTo(0)));
    }

    private static boolean isServiceAccountNotExists() {
        return UtilsTest.class
            .getClassLoader()
            .getResource("isServiceAccountNotExists") == null;
    }
}