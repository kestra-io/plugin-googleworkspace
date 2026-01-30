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
        title = "Destination folders",
        description = "Parent folder IDs; leave empty for My Drive root"
    )
    protected Property<List<String>> parents;

    @Schema(
        title = "File name",
        description = "Display name; not necessarily unique inside a folder"
    )
    protected Property<String> name;

    @Schema(
        title = "File description",
        description = "Optional short description; supports templating"
    )
    @PluginProperty(dynamic = true)
    protected String description;

    @Schema(
        title = "MIME type",
        description = "Explicit MIME type; Drive auto-detects if omitted. Google MIME types import content when possible. Supported import formats: https://developers.google.com/drive/api/v3/mime-types"
    )
    protected Property<String> mimeType;

    @Schema(
        title = "Shared Drive ID",
        description = "Target Shared Drive (teamDriveId) when writing outside My Drive"
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
