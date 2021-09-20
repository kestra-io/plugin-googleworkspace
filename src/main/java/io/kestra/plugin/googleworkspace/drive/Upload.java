package io.kestra.plugin.googleworkspace.drive;

import com.google.api.client.http.FileContent;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import java.io.FileOutputStream;
import java.net.URI;
import java.util.List;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Upload a csv and convert it to sheet format",
            code = {
                "from: \"{{ inputs.file }}\"",
                "to: \"1HuxzpLt1b0111MuKMgy8wAv-m9Myc1E_\"",
                "name: \"My awesome CSV\"",
                "contentType: \"text/csv\"",
                "mineType: \"application/vnd.google-apps.spreadsheet\""
            }
        )
    }
)
@Schema(
    title = "Upload a file in a Drive folder."
)
public class Upload extends AbstractCreate implements RunnableTask<Upload.Output> {
    @Schema(
        title = "The file to copy"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "The content-type of the file.",
        description = "a valid [RFC2045](https://datatracker.ietf.org/doc/html/rfc2045) like `text/csv`, `application/msword`, ... "
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String contentType;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Drive service = this.connection(runContext);
        Logger logger = runContext.logger();
        URI from = URI.create(runContext.render(this.from));

        File fileMetadata = this.file(runContext);

        java.io.File tempFile = runContext.tempFile().toFile();
        try (FileOutputStream out = new FileOutputStream(tempFile)) {
            IOUtils.copy(runContext.uriToInputStream(from), out);
        }

        FileContent fileContent = new FileContent(runContext.render(contentType), tempFile);

        File file = service
            .files()
            .create(fileMetadata, fileContent)
            .setFields("id, name, size, version, createdTime, parents, trashed")
            .execute();

        runContext.metric(Counter.of("size", file.size()));
        logger.debug("Upload from '{}' to '{}'", from, fileMetadata.getParents());

        return Output
            .builder()
            .file(io.kestra.plugin.googleworkspace.drive.models.File.of(file))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The file metadata uploaded"
        )
        private final io.kestra.plugin.googleworkspace.drive.models.File file;
    }
}
