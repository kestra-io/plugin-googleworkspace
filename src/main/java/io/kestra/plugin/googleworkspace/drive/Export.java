package io.kestra.plugin.googleworkspace.drive;

import com.google.api.services.drive.Drive;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
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
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: googleworkspace_drive_export
                namespace: company.team

                tasks:
                  - id: export
                    type: io.kestra.plugin.googleworkspace.drive.Export
                    fileId: "1Dkd3W0OQo-wxz1rrORLP7YGSj6EBLEg74fiTdbJUIQE"
                """
        )
    },
    metrics = {
        @Metric(
            name = "size",
            type = COUNTER.type,
            unit = "count",
            description = "Number of files returned by the list query"
        )
    }
)
@Schema(
    title = "Export a Google Workspace document into a downloadable file format, then download the file.",
    description = "Possible Google Workspace documents to export include Google Docs, Sheets, and Slides, and they can be exported as Word, Excel, Powerpoint files, and more."
)
public class Export extends AbstractDrive implements RunnableTask<Export.Output> {
    @Schema(
        title = "The file id to copy"
    )
    @NotNull
    private Property<String> fileId;

    @Schema(
        title = "The content-type of the file",
        description = "a valid [RFC2045](https://datatracker.ietf.org/doc/html/rfc2045) like `text/csv`, `application/msword`, etc. "
    )
    @NotNull
    private Property<String> contentType;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Drive service = this.connection(runContext);
        Logger logger = runContext.logger();
        String fileId = runContext.render(this.fileId).as(String.class).orElseThrow();

        File tempFile = runContext.workingDir().createTempFile().toFile();

        try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            com.google.api.services.drive.model.File file = service
                .files()
                .get(fileId)
                .setFields("id, name, size, version, createdTime, parents, trashed, mimeType")
                .setSupportsAllDrives(true)
                .execute();
            Drive.Files.Export export = service.files().export(fileId, runContext.render(contentType).as(String.class).orElseThrow());

            export.executeMediaAndDownloadTo(outputStream);
            outputStream.flush();

            runContext.metric(Counter.of("size", file.size()));

            // For Google apps files such as doc, spreadsheet, csv - file.getSize() will return: null, 1 or 1024 (Which is incorrect value)
            // Hardcoding file size from tempFile size in bytes that have been exported/created
            file.setSize(tempFile.length());

            logger.debug("Download from '{}'", fileId);

            return Output
                .builder()
                .uri(runContext.storage().putFile(tempFile))
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
