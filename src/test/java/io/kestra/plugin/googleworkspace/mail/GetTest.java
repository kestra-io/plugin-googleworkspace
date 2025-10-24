package io.kestra.plugin.googleworkspace.mail;

import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.Message;
import com.google.api.services.gmail.model.MessagePart;
import com.google.api.services.gmail.model.MessagePartHeader;
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
 * Unit test for the Gmail "Get" task.
 * This test mocks Gmail.Users.Messages.Get#execute() to avoid real network calls.
 */
@KestraTest
class GetTest {

    // ---- Injected Kestra test context ----
    @Inject
    private RunContextFactory runContextFactory;

    // ---- Mock handle ----
    private static MockedConstruction<Gmail.Users.Messages.Get> gmailGetMock;

    // ---- Mock constants ----
    private static final String MOCK_CLIENT_ID = "mock-client-id";
    private static final String MOCK_CLIENT_SECRET = "mock-client-secret";
    private static final String MOCK_REFRESH_TOKEN = "mock-refresh-token";

    @BeforeAll
    static void setupMocks() throws Exception {
        // Mock Gmail.Users.Messages.Get constructor and its execute() method
        gmailGetMock = Mockito.mockConstruction(Gmail.Users.Messages.Get.class, (mock, context) -> {
            MessagePart payload = new MessagePart().setHeaders(List.of(
                new MessagePartHeader().setName("Subject").setValue("Mocked Subject"),
                new MessagePartHeader().setName("From").setValue("mock@kestra.io"),
                new MessagePartHeader().setName("To").setValue("test@kestra.io")
            ));

            Message fakeMessage = new Message()
                .setId("mock-message-id")
                .setThreadId("mock-thread-id")
                .setSnippet("Mocked snippet content")
                .setPayload(payload);

            Mockito.when(mock.execute()).thenReturn(fakeMessage);
        });

        System.out.println("✅ Mocked Gmail.Users.Messages.Get#execute()");
    }

    @AfterAll
    static void tearDownMocks() {
        if (gmailGetMock != null) {
            gmailGetMock.close();
            System.out.println("🧹 Mock for Gmail.Users.Messages.Get released");
        }
    }

    @Test
    void getMessageFull() throws Exception {
        runAndAssertMessage("full");
    }

    @Test
    void getMessageMetadata() throws Exception {
        runAndAssertMessage("metadata");
    }

    private void runAndAssertMessage(String format) throws Exception {
        RunContext runContext = runContextFactory.of();

        String messageId = "mock-message-id";

        Get task = Get.builder()
            .clientId(Property.ofValue(MOCK_CLIENT_ID))
            .clientSecret(Property.ofValue(MOCK_CLIENT_SECRET))
            .refreshToken(Property.ofValue(MOCK_REFRESH_TOKEN))
            .messageId(Property.ofValue(messageId))
            .format(Property.ofValue(format))
            .build();

        Get.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getMessage(), is(notNullValue()));
        assertThat(output.getMessage().getId(), is(messageId));
        assertThat(output.getMessage().getSubject(), is("Mocked Subject"));
    }
}
