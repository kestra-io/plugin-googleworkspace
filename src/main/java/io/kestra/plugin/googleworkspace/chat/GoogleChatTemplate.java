package io.kestra.plugin.googleworkspace.chat;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.IOUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class GoogleChatTemplate extends GoogleChatIncomingWebhook {

    @Schema(
        title = "Template path for payload",
        description = "Classpath PEB template rendered into the Chat payload",
        hidden = true
    )
    protected Property<String> templateUri;

    @Schema(
        title = "Variables used to render template",
        description = "Map rendered into the template context"
    )
    protected Property<Map<String, Object>> templateRenderMap;

    @Schema(
        title = "Text override for message",
        description = "Plain text that overrides or supplements the template output"
    )
    protected Property<String> text;

    @SuppressWarnings("unchecked")
    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        Map<String, Object> map = new HashMap<>();

        final var renderedUri = runContext.render(this.templateUri).as(String.class);
        if (renderedUri.isPresent()) {
            String template = IOUtils.toString(
                Objects.requireNonNull(this.getClass()
                    .getClassLoader()
                    .getResourceAsStream(renderedUri.get())),
                StandardCharsets.UTF_8
            );

            String render = runContext.render(template, templateRenderMap != null ?
                runContext.render(templateRenderMap).asMap(String.class, Object.class) :
                Map.of()
            );
            map = (Map<String, Object>) JacksonMapper.ofJson().readValue(render, Object.class);
        }

        if (runContext.render(this.text).as(String.class).isPresent()) {
            map.put("text", runContext.render(this.text).as(String.class).get());
        }

        this.payload = Property.ofValue(JacksonMapper.ofJson().writeValueAsString(map));

        return super.run(runContext);
    }

}
