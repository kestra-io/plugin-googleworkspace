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

import java.io.*;
import java.net.URI;
import jakarta.validation.constraints.NotNull;

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
    title = "Download a file in a Drive folder."
)
public class Download extends AbstractDrive implements RunnableTask<Download.Output> {
    @Schema(
        title = "The file id to copy"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String fileId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Drive service = this.connection(runContext);
        Logger logger = runContext.logger();
        String fileId = runContext.render(this.fileId);

        File tempFile = runContext.tempFile().toFile();
        try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            Drive.Files.Get get = service.files().get(fileId);
            com.google.api.services.drive.model.File file = get
                .setFields("id, name, size, version, createdTime, parents, trashed, mimeType")
                .setSupportsTeamDrives(true)
                .execute();

            get.executeMediaAndDownloadTo(outputStream);
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
