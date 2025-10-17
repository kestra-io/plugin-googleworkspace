package io.kestra.plugin.googleworkspace.drive;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.FileList;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.googleworkspace.drive.models.File;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "List subfolder in a Drive folder",
            full = true,
            code = """
                id: googleworkspace_drive_list
                namespace: company.team

                tasks:
                  - id: list
                    type: io.kestra.plugin.googleworkspace.drive.List
                    query: |
                      mimeType = 'application/vnd.google-apps.folder'
                      and '1z2GZgLEX12BN9zbVE6TodrCHyTRMj_ka' in parents
                """
        )
    },
    metrics = {
        @Metric(
            name= "size",
            type= Counter.TYPE,
            unit= "count",
            description = "Number of files returned by the list query"
        )
    }
)
@Schema(
    title = "List files in a Google Drive folder."
)
public class List extends AbstractDrive implements RunnableTask<List.Output> {
    @Schema(
        title = "Query operators to filter results",
        description = "For more details refer to [Search Files API](https://developers.google.com/drive/api/v3/search-files).\n" +
            "If not defined, the task will list all files that the service account have access."
    )
    private Property<String> query;

    @Schema(
        title = "List of bodies of items (files/documents) applicable to the query",
        description = "'allTeamDrives' must" +
            " be combined with 'user'; all other values must be used in isolation. Prefer 'user' or 'teamDrive' " +
            "to 'allTeamDrives' for efficiency."
    )
    private Property<java.util.List<Corpora>> corpora;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Drive service = this.connection(runContext);
        Logger logger = runContext.logger();

        String query = this.query != null ? runContext.render(this.query).as(String.class).orElseThrow() : null;

        Drive.Files.List list = service.files()
            .list()
            .setFields("nextPageToken, files(id, name, size, version, createdTime, parents, trashed)")
            .setQ(query);

        if (this.corpora != null) {
            list.setCorpora(runContext.render(this.corpora).asList(Corpora.class)
                .stream()
                .map(Enum::name)
                .collect(Collectors.joining(","))
            );
        }

        java.util.List<File> files = this.list(list);

        runContext.metric(Counter.of("size", files.size()));
        logger.debug("Found '{}' files from  query'{}'", files.size(), query);

        return Output
            .builder()
            .files(files)
            .build();
    }

    protected java.util.List<File> list(Drive.Files.List list) throws IOException {
        java.util.List<File> result = new ArrayList<>();

        String pageToken = null;
        do {
            FileList fileList = list.setPageToken(pageToken)
                .setSupportsAllDrives(true)
                .setIncludeItemsFromAllDrives(true)
                .execute();

            result.addAll(fileList
                .getFiles()
                .stream()
                .map(io.kestra.plugin.googleworkspace.drive.models.File::of)
                .collect(Collectors.toList())
            );

            pageToken = fileList.getNextPageToken();
        } while (pageToken != null);

        return result;
    }

    public enum Corpora {
        user,
        domain,
        teamDrive,
        allTeamDrives
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The list of files"
        )
        private final java.util.List<File> files;
    }
}
