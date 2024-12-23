package io.kestra.plugin.googleworkspace.drive;

import com.google.api.services.drive.Drive;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

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
                id: googleworkspace_drive_delete
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.googleworkspace.drive.Delete
                    fileId: "1Dkd3W0OQo-wxz1rrORLP7YGSj6EBLEg74fiTdbJUIQE"
                """
        )
    }
)
@Schema(
    title = "Delete a file on a Drive folder."
)
public class Delete extends AbstractDrive implements RunnableTask<Delete.Output> {
    @Schema(
        title = "The file id to delete"
    )
    private Property<String> fileId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Drive service = this.connection(runContext);
        Logger logger = runContext.logger();
        String id = runContext.render(this.fileId).as(String.class).orElse(null);

        Void execute = service
            .files()
            .delete(id)
            .setSupportsAllDrives(true)
            .execute();

        logger.debug("Deleted '{}'", id);

        return Output
            .builder()
            .fileId(id)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The file id deleted"
        )
        private final String fileId;
    }
}
