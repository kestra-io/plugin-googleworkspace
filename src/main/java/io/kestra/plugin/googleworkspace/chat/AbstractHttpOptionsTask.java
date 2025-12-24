package io.kestra.plugin.googleworkspace.chat;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.TimeoutConfiguration;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractHttpOptionsTask extends Task implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Options",
        description = "The options to set to customize the HTTP client"
    )
    @PluginProperty(dynamic = true)
    protected RequestOptions options;

    protected HttpConfiguration httpClientConfigurationWithOptions() throws IllegalVariableEvaluationException {
        HttpConfiguration.HttpConfigurationBuilder configuration = HttpConfiguration.builder();

        if (this.options != null) {

            configuration
                .timeout(TimeoutConfiguration.builder()
                    .connectTimeout(this.options.getConnectTimeout())
                    .readIdleTimeout(this.options.getReadIdleTimeout())
                .build())
                .defaultCharset(this.options.getDefaultCharset());
        }

        return configuration.build();
    }

    protected HttpRequest.HttpRequestBuilder createRequestBuilder(
        RunContext runContext) throws IllegalVariableEvaluationException {

        HttpRequest.HttpRequestBuilder builder = HttpRequest.builder();

        if (this.options != null && this.options.getHeaders() != null) {
            Map<String, String> headers = runContext.render(this.options.getHeaders())
                .asMap(String.class, String.class);

            if (headers != null) {
                headers.forEach(builder::addHeader);
            }
        }
        return builder;
    }

    @Getter
    @Builder
    public static class RequestOptions {
        @Schema(title = "The time allowed to establish a connection to the server before failing.")
        private final Property<Duration> connectTimeout;

        @Schema(title = "The maximum time allowed for reading data from the server before failing.")
        @Builder.Default
        private final Property<Duration> readTimeout = Property.ofValue(Duration.ofSeconds(10));

        @Schema(title = "The time allowed for a read connection to remain idle before closing it.")
        @Builder.Default
        private final Property<Duration> readIdleTimeout = Property.ofValue(Duration.of(5, ChronoUnit.MINUTES));

        @Schema(title = "The time an idle connection can remain in the client's connection pool before being closed.")
        @Builder.Default
        private final Property<Duration> connectionPoolIdleTimeout = Property.ofValue(Duration.ofSeconds(0));

        @Schema(title = "The maximum content length of the response.")
        @Builder.Default
        private final Property<Integer> maxContentLength = Property.ofValue(1024 * 1024 * 10);

        @Schema(title = "The default charset for the request.")
        @Builder.Default
        private final Property<Charset> defaultCharset = Property.ofValue(StandardCharsets.UTF_8);

        @Schema(
            title = "HTTP headers",
            description = "HTTP headers to include in the request"
        )
        public Property<Map<String,String>> headers;
    }
}
