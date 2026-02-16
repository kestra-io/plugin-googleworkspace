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
public abstract class AbstractChatConnection extends Task implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Configure HTTP client options",
        description = "Optional HTTP settings (timeouts, charset, headers) applied to webhook calls"
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
        @Schema(
            title = "Connect timeout for webhook calls",
            description = "Max time to open the connection before failing"
        )
        private final Property<Duration> connectTimeout;

        @Schema(
            title = "Read timeout for responses",
            description = "Max time to read data before failing; default PT10S"
        )
        @Builder.Default
        private final Property<Duration> readTimeout = Property.ofValue(Duration.ofSeconds(10));

        @Schema(
            title = "Idle timeout during read",
            description = "Idle time on an open connection before closing; default PT5M"
        )
        @Builder.Default
        private final Property<Duration> readIdleTimeout = Property.ofValue(Duration.of(5, ChronoUnit.MINUTES));

        @Schema(
            title = "Connection pool idle timeout",
            description = "How long an idle connection stays in the pool; default PT0S"
        )
        @Builder.Default
        private final Property<Duration> connectionPoolIdleTimeout = Property.ofValue(Duration.ofSeconds(0));

        @Schema(
            title = "Maximum response body size",
            description = "Maximum response body length; default 10MB"
        )
        @Builder.Default
        private final Property<Integer> maxContentLength = Property.ofValue(1024 * 1024 * 10);

        @Schema(
            title = "Default charset for requests",
            description = "Request charset; default UTF-8"
        )
        @Builder.Default
        private final Property<Charset> defaultCharset = Property.ofValue(StandardCharsets.UTF_8);

        @Schema(
            title = "Custom HTTP request headers",
            description = "Optional headers to add to every request"
        )
        public Property<Map<String,String>> headers;
    }
}
