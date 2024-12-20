package io.kestra.plugin.googleworkspace.drive;

import com.google.api.services.drive.model.File;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractCreate extends AbstractDrive {
    @Schema(
        title = "The destination path"
    )
    protected Property<List<String>> parents;

    @Schema(
        title = "The name of the file",
        description = "This is not necessarily unique within a folder"
    )
    protected Property<String> name;

    @Schema(
        title = "A short description of the file."
    )
    @PluginProperty(dynamic = true)
    protected String description;

    @Schema(
        title = "The MIME type of the file.",
        description = "Drive will attempt to automatically detect an appropriate value from uploaded content if no " +
            "value is provided. The value cannot be changed unless a new revision is uploaded. If a file is created " +
            "with a Google Doc MIME type, the uploaded content will be imported if possible. " +
            "The supported import formats are published [here](https://developers.google.com/drive/api/v3/mime-types)."
    )
    protected Property<String> mimeType;

    @Schema(
        title = "ID of the Team Drive the file resides in."
    )
    protected Property<String> teamDriveId;

    protected File file(RunContext runContext) throws IllegalVariableEvaluationException {
        File fileMetadata = new File();

        if (this.name != null) {
            fileMetadata.setName(runContext.render(this.name).as(String.class).orElseThrow());
        }

        var renderedParents = runContext.render(this.parents).asList(String.class);
        if (!renderedParents.isEmpty()) {
            fileMetadata.setParents(renderedParents);
        }

        if (mimeType != null) {
            fileMetadata.setMimeType(runContext.render(mimeType).as(String.class).orElseThrow());
        }

        if (teamDriveId != null) {
            fileMetadata.setTeamDriveId(runContext.render(teamDriveId).as(String.class).orElseThrow());
        }

        if (description != null) {
            fileMetadata.setDescription(runContext.render(description));
        }

        return fileMetadata;
    }
}
