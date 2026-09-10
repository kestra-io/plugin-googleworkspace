package io.kestra.plugin.googleworkspace.chat;

import java.net.URI;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.is;

class WebhookUrlTest {
    private static String param(String query, String name) {
        return GoogleChatIncomingWebhook
            .queryParameters(URI.create("https://chat.googleapis.com/v1/spaces/AAAA/messages?" + query))
            .get(name);
    }

    @Test
    void readsKeyAndToken() {
        assertThat(param("key=AIzaSyAbC&token=xYz", "key"), is("AIzaSyAbC"));
        assertThat(param("key=AIzaSyAbC&token=xYz", "token"), is("xYz"));
    }

    /** URLDecoder would turn this into a space and silently corrupt the credential. */
    @Test
    void keepsALiteralPlusInAToken() {
        assertThat(param("token=ab+cd", "token"), is("ab+cd"));
        assertThat(param("token=ab%2Bcd", "token"), is("ab+cd"));
    }

    @Test
    void decodesPercentEscapes() {
        assertThat(param("token=ab%3Dcd", "token"), is("ab=cd"));
        assertThat(param("token=a%20b", "token"), is("a b"));
    }

    @Test
    void toleratesNoQuery() {
        assertThat(
            GoogleChatIncomingWebhook.queryParameters(URI.create("https://chat.googleapis.com/v1/spaces/A/messages")),
            is(anEmptyMap())
        );
    }
}
