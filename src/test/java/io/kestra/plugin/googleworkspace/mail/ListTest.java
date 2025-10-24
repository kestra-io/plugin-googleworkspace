package io.kestra.plugin.googleworkspace.mail;

import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.ListMessagesResponse;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Unit test for the Gmail "List" task.
 * This test mocks Gmail.Users.Messages.List#execute() to avoid real API calls.
 */
@KestraTest
class ListTest {

    @Inject
    private RunContextFactory runContextFactory;

    private static MockedConstruction<Gmail.Users.Messages.List> gmailListMock;

    private static final String MOCK_CLIENT_ID = "mock-client-id";
    private static final String MOCK_CLIENT_SECRET = "mock-client-secret";
    private static final String MOCK_REFRESH_TOKEN = "mock-refresh-token";

    @BeforeAll
    static void setupMocks() throws Exception {
        gmailListMock = Mockito.mockConstruction(Gmail.Users.Messages.List.class, (mock, context) -> {
            // Prepare fake Gmail list response
            Message msg1 = new Message().setId("mock-message-1");
            Message msg2 = new Message().setId("mock-message-2");

            ListMessagesResponse fakeResponse = new ListMessagesResponse()
                .setMessages(java.util.List.of(msg1, msg2))
                .setResultSizeEstimate(2L);

            Mockito.when(mock.execute()).thenReturn(fakeResponse);
        });

        System.out.println("✅ Mocked Gmail.Users.Messages.List#execute()");
    }

    @AfterAll
    static void tearDownMocks() {
        if (gmailListMock != null) {
            gmailListMock.close();
            System.out.println("🧹 Mock for Gmail.Users.Messages.List released");
        }
    }

    @Test
    void listMessages() throws Exception {
        RunContext runContext = runContextFactory.of();

        List task = List.builder()
            .clientId(Property.ofValue(MOCK_CLIENT_ID))
            .clientSecret(Property.ofValue(MOCK_CLIENT_SECRET))
            .refreshToken(Property.ofValue(MOCK_REFRESH_TOKEN))
            .maxResults(Property.ofValue(10))
            .build();

        List.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getMessages(), is(notNullValue()));
        assertThat(output.getMessages().size(), greaterThanOrEqualTo(1));
    }

    @Test
    void listUnreadMessages() throws Exception {
        RunContext runContext = runContextFactory.of();

        List task = List.builder()
            .clientId(Property.ofValue(MOCK_CLIENT_ID))
            .clientSecret(Property.ofValue(MOCK_CLIENT_SECRET))
            .refreshToken(Property.ofValue(MOCK_REFRESH_TOKEN))
            .query(Property.ofValue("is:unread"))
            .maxResults(Property.ofValue(5))
            .build();

        List.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getMessages(), is(notNullValue()));
        assertThat(output.getMessages().size(), greaterThanOrEqualTo(1));
    }

    @Test
    void listMessagesWithLabels() throws Exception {
        RunContext runContext = runContextFactory.of();

        List task = List.builder()
            .clientId(Property.ofValue(MOCK_CLIENT_ID))
            .clientSecret(Property.ofValue(MOCK_CLIENT_SECRET))
            .refreshToken(Property.ofValue(MOCK_REFRESH_TOKEN))
            .labelIds(Property.ofValue(java.util.List.of("INBOX")))
            .maxResults(Property.ofValue(15))
            .build();

        List.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getMessages(), is(notNullValue()));
        assertThat(output.getMessages().size(), greaterThanOrEqualTo(1));
    }
}
