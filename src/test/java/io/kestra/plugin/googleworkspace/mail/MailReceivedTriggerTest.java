package io.kestra.plugin.googleworkspace.mail;

import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.ListMessagesResponse;
import com.google.api.services.gmail.model.Message;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.googleworkspace.mail.models.EmailMetadata;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Unit test for MailReceivedTrigger.
 * This test mocks Gmail.Users.Messages.List and Get to avoid any real API calls.
 */
@KestraTest
class MailReceivedTriggerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    private static MockedConstruction<Gmail.Users.Messages.List> gmailListMock;
    private static MockedConstruction<Gmail.Users.Messages.Get> gmailGetMock;

    private static final String MOCK_CLIENT_ID = "mock-client-id";
    private static final String MOCK_CLIENT_SECRET = "mock-client-secret";
    private static final String MOCK_REFRESH_TOKEN = "mock-refresh-token";

    @BeforeAll
    static void setupMocks() throws Exception {
        // --- Mock Gmail.Users.Messages.List constructor + execute() ---
        gmailListMock = Mockito.mockConstruction(Gmail.Users.Messages.List.class, (mock, context) -> {
            Message fakeMsg = new Message()
                .setId("mock-message-id")
                .setThreadId("mock-thread-id");

            ListMessagesResponse fakeResponse = new ListMessagesResponse()
                .setMessages(List.of(fakeMsg))
                .setResultSizeEstimate(1L);

            Mockito.when(mock.execute()).thenReturn(fakeResponse);
        });

        // --- Mock Gmail.Users.Messages.Get constructor + execute() ---
        gmailGetMock = Mockito.mockConstruction(Gmail.Users.Messages.Get.class, (mock, context) -> {
            Message fakeFullMessage = new Message()
                .setId("mock-message-id")
                .setThreadId("mock-thread-id")
                .setInternalDate(System.currentTimeMillis())
                .setSnippet("Mocked Snippet")
                .setPayload(new com.google.api.services.gmail.model.MessagePart()
                    .setHeaders(List.of(
                        new com.google.api.services.gmail.model.MessagePartHeader().setName("Subject").setValue("Mocked Subject"),
                        new com.google.api.services.gmail.model.MessagePartHeader().setName("From").setValue("mock@kestra.io"),
                        new com.google.api.services.gmail.model.MessagePartHeader().setName("To").setValue("test@kestra.io")
                    )));

            // Return "this" mock when setFormat() is called
            Mockito.when(mock.setFormat(Mockito.anyString())).thenReturn(mock);
            // Return fake Gmail message when execute() is called
            Mockito.when(mock.execute()).thenReturn(fakeFullMessage);
        });

        System.out.println("✅ Gmail.Users.Messages.List & Get mocks successfully registered");
    }

    @AfterAll
    static void tearDownMocks() {
        if (gmailListMock != null) gmailListMock.close();
        if (gmailGetMock != null) gmailGetMock.close();
        System.out.println("🧹 Gmail mocks released");
    }

    @Test
    void basicTrigger() throws Exception {
        MailReceivedTrigger trigger = MailReceivedTrigger.builder()
            .id("test-trigger-" + IdUtils.create())
            .type(MailReceivedTrigger.class.getName())
            .clientId(Property.ofValue(MOCK_CLIENT_ID))
            .clientSecret(Property.ofValue(MOCK_CLIENT_SECRET))
            .refreshToken(Property.ofValue(MOCK_REFRESH_TOKEN))
            .interval(Duration.ofMinutes(1))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        // Assert execution succeeded
        assertThat(execution.isPresent(), is(true));

        List<EmailMetadata> messages = (List<EmailMetadata>) execution.get()
            .getTrigger().getVariables().get("messages");

        assertThat(messages, notNullValue());
        assertThat(messages.size(), greaterThanOrEqualTo(1));

        EmailMetadata firstMessage = messages.getFirst();
        assertThat(firstMessage.getId(), is("mock-message-id"));
        assertThat(firstMessage.getSubject(), is("Mocked Subject"));
        assertThat(firstMessage.getFrom(), is("mock@kestra.io"));
        assertThat(firstMessage.getTo(), contains("test@kestra.io"));
    }

    @Test
    void noNewMessages() throws Exception {
        // Instead of creating a new construction mock, just reconfigure the existing one
        gmailListMock.close(); // Unregister the previous mock first
        gmailListMock = Mockito.mockConstruction(Gmail.Users.Messages.List.class, (mock, context) -> {
            ListMessagesResponse emptyResponse = new ListMessagesResponse()
                .setMessages(null)
                .setResultSizeEstimate(0L);
            Mockito.when(mock.execute()).thenReturn(emptyResponse);
        });

        MailReceivedTrigger trigger = MailReceivedTrigger.builder()
            .id("test-trigger-" + IdUtils.create())
            .type(MailReceivedTrigger.class.getName())
            .clientId(Property.ofValue(MOCK_CLIENT_ID))
            .clientSecret(Property.ofValue(MOCK_CLIENT_SECRET))
            .refreshToken(Property.ofValue(MOCK_REFRESH_TOKEN))
            .query(Property.ofValue("subject:NONEXISTENT_SUBJECT_" + IdUtils.create()))
            .interval(Duration.ofMinutes(1))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        // Expect no execution triggered
        assertThat(execution.isPresent(), is(false));
    }
}
