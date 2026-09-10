package io.kestra.plugin.googleworkspace.chat;

import java.util.HashMap;
import java.util.Map;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;

@Controller
public class FakeWebhookController {
    public static String data;
    public static Map<String, String> headers = new HashMap<>();
    public static Map<String, String> queryParameters = new HashMap<>();

    /** Stubs the real spaces.messages.create path the Chat SDK builds from a webhook URL. */
    @Post("/v1/spaces/{space}/messages")
    @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_FORM_URLENCODED })
    public HttpResponse<String> createMessage(HttpRequest<?> request, String space, @Body String data) {
        FakeWebhookController.data = data;
        request.getParameters().forEach((name, values) ->
        {
            if (!values.isEmpty()) {
                queryParameters.put(name, values.get(0));
            }
        });

        return HttpResponse.ok("{\"name\":\"" + space + "/messages/unit-test\"}")
            .contentType(MediaType.APPLICATION_JSON);
    }

    @Post("/webhook-unit-test")
    @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_FORM_URLENCODED })
    public HttpResponse<String> post(@Body String data) {
        FakeWebhookController.data = data;
        return HttpResponse.ok("ok");
    }

    @Post("/webhook-unit-test/with-headers")
    @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_FORM_URLENCODED })
    public HttpResponse<String> postWithHeaders(HttpRequest<?> request, @Body String data) {

        FakeWebhookController.data = data;
        request.getHeaders().forEach((name, values) ->
        {
            if (!values.isEmpty()) {
                headers.put(name, values.get(0));
            }
        });

        return HttpResponse.ok("ok");
    }
}
