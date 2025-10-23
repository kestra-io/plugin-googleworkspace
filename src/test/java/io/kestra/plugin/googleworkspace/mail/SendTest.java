package io.kestra.plugin.googleworkspace.mail;

import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.Message;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit test for the Gmail "Send" task.
 * This test mocks Gmail.Users.Messages#send(...).execute() to avoid real API calls.
 */
@KestraTest
class SendTest {

    @Inject
    private RunContextFactory runContextFactory;

    private static MockedConstruction<Gmail.Users.Messages> gmailMessagesMock;

    private static final String MOCK_CLIENT_ID = "mock-client-id";
    private static final String MOCK_CLIENT_SECRET = "mock-client-secret";
    private static final String MOCK_REFRESH_TOKEN = "mock-refresh-token";

    @BeforeAll
    static void setupMocks() throws Exception {
        // Mock Gmail.Users.Messages constructor and send() + execute() behavior
        gmailMessagesMock = Mockito.mockConstruction(Gmail.Users.Messages.class, (mock, context) -> {
            Gmail.Users.Messages.Send sendMock = Mockito.mock(Gmail.Users.Messages.Send.class);

            Message fakeSentMessage = new Message()
                .setId("mock-message-id")
                .setThreadId("mock-thread-id");

            Mockito.when(sendMock.execute()).thenReturn(fakeSentMessage);
            Mockito.when(mock.send(Mockito.anyString(), Mockito.any(Message.class))).thenReturn(sendMock);
        });

        System.out.println("✅ Mocked Gmail.Users.Messages#send().execute()");
    }

    @AfterAll
    static void tearDownMocks() {
        if (gmailMessagesMock != null) {
            gmailMessagesMock.close();
            System.out.println("🧹 Mock for Gmail.Users.Messages released");
        }
    }

    @Test
    void sendSimpleEmail() throws Exception {
        RunContext runContext = runContextFactory.of();

        Send task = Send.builder()
            .clientId(Property.ofValue(MOCK_CLIENT_ID))
            .clientSecret(Property.ofValue(MOCK_CLIENT_SECRET))
            .refreshToken(Property.ofValue(MOCK_REFRESH_TOKEN))
            .to(Property.ofValue(List.of("test@example.com")))
            .subject(Property.ofValue("Test Email from Kestra"))
            .textBody(Property.ofValue("This is a test email sent from Kestra Gmail plugin."))
            .build();

        Send.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getMessageId(), is("mock-message-id"));
        assertThat(output.getThreadId(), is("mock-thread-id"));
    }

    @Test
    void sendHtmlEmail() throws Exception {
        RunContext runContext = runContextFactory.of();

        Send task = Send.builder()
            .clientId(Property.ofValue(MOCK_CLIENT_ID))
            .clientSecret(Property.ofValue(MOCK_CLIENT_SECRET))
            .refreshToken(Property.ofValue(MOCK_REFRESH_TOKEN))
            .to(Property.ofValue(List.of("test@example.com")))
            .cc(Property.ofValue(List.of("cc@example.com")))
            .subject(Property.ofValue("HTML Test Email"))
            .htmlBody(Property.ofValue("<h1>Test Email</h1><p>This is an <b>HTML</b> email from Kestra.</p>"))
            .build();

        Send.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getMessageId(), is("mock-message-id"));
        assertThat(output.getThreadId(), is("mock-thread-id"));
    }

    @Test
    void sendMultipartEmail() throws Exception {
        RunContext runContext = runContextFactory.of();

        Send task = Send.builder()
            .clientId(Property.ofValue(MOCK_CLIENT_ID))
            .clientSecret(Property.ofValue(MOCK_CLIENT_SECRET))
            .refreshToken(Property.ofValue(MOCK_REFRESH_TOKEN))
            .to(Property.ofValue(List.of("test@example.com")))
            .subject(Property.ofValue("Multipart Test Email"))
            .textBody(Property.ofValue("This is the plain text version."))
            .htmlBody(Property.ofValue("<p>This is the <em>HTML</em> version.</p>"))
            .build();

        Send.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getMessageId(), is("mock-message-id"));
        assertThat(output.getThreadId(), is("mock-thread-id"));
    }

    private static boolean isServiceAccountNotExists() {
        return SendTest.class
            .getClassLoader()
            .getResource(".gcp-service-account.json") == null;
    }
}
