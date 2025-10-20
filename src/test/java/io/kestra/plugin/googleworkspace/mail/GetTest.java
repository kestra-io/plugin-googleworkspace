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

@KestraTest
@DisabledIf(
    value = "isServiceAccountNotExists",
    disabledReason = "Disabled for CI/CD"
)
class GetTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void getMessage() throws Exception {
        RunContext runContext = runContextFactory.of();

        // First, list messages to get a valid message ID
        List listTask = List.builder()
            .clientId(Property.ofValue(UtilsTest.oauthClientId()))
            .clientSecret(Property.ofValue(UtilsTest.oauthClientSecret()))
            .refreshToken(Property.ofValue(UtilsTest.oauthRefreshToken()))
            .maxResults(Property.ofValue(1))
            .build();

        List.Output listOutput = listTask.run(runContext);

        // Skip test if no messages found
        if (listOutput.getMessages().isEmpty()) {
            return;
        }

        String messageId = listOutput.getMessages().get(0).getId();

        Get task = Get.builder()
            .clientId(Property.ofValue(UtilsTest.oauthClientId()))
            .clientSecret(Property.ofValue(UtilsTest.oauthClientSecret()))
            .refreshToken(Property.ofValue(UtilsTest.oauthRefreshToken()))
            .messageId(Property.ofValue(messageId))
            .format(Property.ofValue("full"))
            .build();

        Get.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getMessage(), is(notNullValue()));
        assertThat(output.getMessage().getId(), is(messageId));
    }

    @Test
    void getMessageMetadata() throws Exception {
        RunContext runContext = runContextFactory.of();

        // First, list messages to get a valid message ID
        List listTask = List.builder()
            .clientId(Property.ofValue(UtilsTest.oauthClientId()))
            .clientSecret(Property.ofValue(UtilsTest.oauthClientSecret()))
            .refreshToken(Property.ofValue(UtilsTest.oauthRefreshToken()))
            .maxResults(Property.ofValue(1))
            .build();

        List.Output listOutput = listTask.run(runContext);

        // Skip test if no messages found
        if (listOutput.getMessages().isEmpty()) {
            return;
        }

        String messageId = listOutput.getMessages().get(0).getId();

        Get task = Get.builder()
            .clientId(Property.ofValue(UtilsTest.oauthClientId()))
            .clientSecret(Property.ofValue(UtilsTest.oauthClientSecret()))
            .refreshToken(Property.ofValue(UtilsTest.oauthRefreshToken()))
            .messageId(Property.ofValue(messageId))
            .format(Property.ofValue("metadata"))
            .build();

        Get.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getMessage(), is(notNullValue()));
        assertThat(output.getMessage().getId(), is(messageId));
    }

    private static boolean isServiceAccountNotExists() {
        return UtilsTest.class
            .getClassLoader()
            .getResource("isServiceAccountNotExists") == null;
    }
}