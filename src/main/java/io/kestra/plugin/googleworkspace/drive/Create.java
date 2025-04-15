package io.kestra.plugin.googleworkspace.drive;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

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
                id: googleworkspace_drive_create
                namespace: company.team

                tasks:
                  - id: create
                    type: io.kestra.plugin.googleworkspace.drive.Create
                    name: "My Folder"
                    mimeType: "application/vnd.google-apps.folder"
                """
        )
    }
)
@Schema(
    title = "Create a file or a folder in Google Drive."
)
public class Create extends AbstractCreate implements RunnableTask<Create.Output> {
    @Override
    public Output run(RunContext runContext) throws Exception {
        Drive service = this.connection(runContext);
        Logger logger = runContext.logger();

        File fileMetadata = file(runContext);

        File file = service
            .files()
            .create(fileMetadata)
            .setFields("id, name, size, version, createdTime, parents, trashed")
            .setSupportsAllDrives(true)
            .execute();

        logger.debug("Created '{}' in '{}'", file.getName(), file.getParents());

        return Output
            .builder()
            .file(io.kestra.plugin.googleworkspace.drive.models.File.of(file))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The file metadata created"
        )
        private final io.kestra.plugin.googleworkspace.drive.models.File file;
    }
}
