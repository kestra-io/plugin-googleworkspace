package io.kestra.plugin.googleworkspace.drive;

import com.google.api.client.http.FileContent;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
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
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

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
            title = "Upload a csv and convert it to sheet format",
            full = true,
            code = """
                id: googleworkspace_drive_upload
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE
                    description: The file to be uploaded to Google Drive

                tasks:
                  - id: upload
                    type: io.kestra.plugin.googleworkspace.drive.Upload
                    from: "{{ inputs.file }}"
                    parents:
                     - "1HuxzpLt1b0111MuKMgy8wAv-m9Myc1E_"
                    name: "My awesome CSV"
                    contentType: "text/csv"
                    mimeType: "application/vnd.google-apps.spreadsheet"
                """
        ),
        @Example(
            full = true,
            title = "Upload file to Google Drive",
            code = """
                id: upload_google_drive
                namespace: company.team
                
                tasks:
                  - id: download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/orders.csv
                
                  - id: upload
                    type: io.kestra.plugin.googleworkspace.drive.Upload
                    from: "{{ outputs.download.uri }}"
                    parents:
                      - 15OENbAxvonlASDkYyfGBftV2c0fDTmnB
                    name: Orders
                    contentType: text/csv
                    mimeType: application/vnd.google-apps.spreadsheet
                """
        )
    },
    metrics = {
        @Metric(
            name = "size",
            type = Counter.TYPE,
            unit = "count",
            description = "Number of files returned by the list query"
        )
    }
)
@Schema(
    title = "Upload a file to a Google Drive folder."
)
public class Upload extends AbstractCreate implements RunnableTask<Upload.Output> {
    @Schema(
        title = "The file URI to copy"
    )
    @NotNull
    private Property<String> from;

    @Schema(
        title = "The file id to update",
        description = "If not provided, the task will create a new file."
    )
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
        URI from = URI.create(runContext.render(this.from).as(String.class).orElseThrow());

        File fileMetadata = this.file(runContext);

        java.io.File tempFile = runContext.workingDir().createTempFile().toFile();
        try (FileOutputStream out = new FileOutputStream(tempFile)) {
            IOUtils.copy(runContext.storage().getFile(from), out);
        }

        FileContent fileContent = new FileContent(runContext.render(contentType).as(String.class).orElseThrow(), tempFile);

        File file;
        if (this.fileId != null) {
            file = service
                .files()
                .update(runContext.render(this.fileId).as(String.class).orElseThrow(), fileMetadata, fileContent)
                .setFields("id, name, version, createdTime, parents, trashed, mimeType")
                .setSupportsAllDrives(true)
                .execute();
        } else {
            file = service
                .files()
                .create(fileMetadata, fileContent)
                .setFields("id, name, version, createdTime, parents, trashed, mimeType")
                .setSupportsAllDrives(true)
                .execute();
        }

        runContext.metric(Counter.of("size", file.size()));
        logger.debug("Upload from '{}' to '{}'", from, fileMetadata.getParents());

        // For Google apps files such as doc, spreadsheet, csv - file.getSize() will return: null, 1 or 1024 (Which is incorrect value)
        // Hardcoding file size from tempFile size in bytes that have been exported/created
        file.setSize(tempFile.length());

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
