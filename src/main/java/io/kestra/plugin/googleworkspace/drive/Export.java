package io.kestra.plugin.googleworkspace.drive;

import com.google.api.services.drive.Drive;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            code = {
                "fileId: \"1Dkd3W0OQo-wxz1rrORLP7YGSj6EBLEg74fiTdbJUIQE\""
            }
        )
    }
)
@Schema(
    title = "Export a file in a Drive folder."
)
public class Export extends AbstractDrive implements RunnableTask<Export.Output> {
    @Schema(
        title = "The file id to copy"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String fileId;

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
        String fileId = runContext.render(this.fileId);

        File tempFile = runContext.tempFile().toFile();

        try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            com.google.api.services.drive.model.File file = service
                .files()
                .get(fileId)
                .setSupportsTeamDrives(true)
                .execute();
            Drive.Files.Export export = service.files().export(fileId, runContext.render(contentType));

            export.executeMediaAndDownloadTo(outputStream);
            outputStream.flush();

            runContext.metric(Counter.of("size", file.size()));

            logger.debug("Download from '{}'", fileId);

            return Output
                .builder()
                .uri(runContext.putTempFile(tempFile))
                .file(io.kestra.plugin.googleworkspace.drive.models.File.of(file))
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The url of the downloaded file on kestra storage "
        )
        private final URI uri;

        @Schema(
            title = "The file metadata uploaded"
        )
        private final io.kestra.plugin.googleworkspace.drive.models.File file;
    }
}
