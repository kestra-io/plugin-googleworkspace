package io.kestra.plugin.googleworkspace.drive;

import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.User;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.ExecutionTrigger;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger that listens for new files created in a Google Drive folder",
    description = "Monitors a specified folder for newly created files and emits an event for each new file detected. " +
        "The trigger uses polling to check for new files based on their creation time."
)
@Plugin(
    examples = {
        @Example(
            title = "Monitor a folder for any new file",
            full = true,
            code = """
                id: google_drive_file_trigger
                namespace: company.team

                tasks:
                  - id: process_file
                    type: io.kestra.plugin.core.log.Log
                    message: "New file created: {{ trigger.name }} ({{ trigger.id }})"

                triggers:
                  - id: watch_folder
                    type: io.kestra.plugin.googleworkspace.drive.FileCreatedTrigger
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT') }}"
                    folderId: "1a2b3c4d5e6f7g8h9i0j"
                    interval: PT5M
                """
        ),
        @Example(
            title = "Monitor for specific MIME types with filters",
            full = true,
            code = """
                id: google_drive_pdf_trigger
                namespace: company.team

                tasks:
                  - id: download_pdf
                    type: io.kestra.plugin.googleworkspace.drive.Download
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT') }}"
                    fileId: "{{ trigger.id }}"

                triggers:
                  - id: watch_pdfs
                    type: io.kestra.plugin.googleworkspace.drive.FileCreatedTrigger
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT') }}"
                    folderId: "1a2b3c4d5e6f7g8h9i0j"
                    mimeTypes:
                      - application/pdf
                      - application/vnd.google-apps.document
                    interval: PT10M
                """
        ),
        @Example(
            title = "Monitor entire Drive with owner filter",
            full = true,
            code = """
                id: google_drive_owner_trigger
                namespace: company.team

                tasks:
                  - id: notify
                    type: io.kestra.plugin.notifications.slack.SlackIncomingWebhook
                    url: "{{ secret('SLACK_WEBHOOK') }}"
                    payload: |
                      {
                        "text": "New file by {{ trigger.owners[0].displayName }}: {{ trigger.name }}"
                      }

                triggers:
                  - id: watch_my_files
                    type: io.kestra.plugin.googleworkspace.drive.FileCreatedTrigger
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT') }}"
                    ownerEmail: "user@company.com"
                    interval: PT15M
                """
        )
    }
)
public class FileCreatedTrigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<FileCreatedTrigger.Output> {

    @Schema(
        title = "The Google Cloud service account key",
        description = "Service account JSON key with access to Google Drive API"
    )
    protected String serviceAccount;

    @Schema(
        title = "The OAuth scopes to request",
        description = "List of OAuth scopes for Drive API access"
    )
    @Builder.Default
    protected Property<List<String>> scopes = Property.ofValue(List.of("https://www.googleapis.com/auth/drive.metadata.readonly"));

    @Schema(
        title = "The folder ID to monitor for new files",
        description = "If not provided, monitors the entire Drive accessible by the service account. " +
            "You can find the folder ID in the Google Drive URL."
    )
    protected Property<String> folderId;

    @Schema(
        title = "List of MIME types to filter",
        description = "Only files matching these MIME types will trigger events. " +
            "Examples: 'application/pdf', 'image/jpeg'" +
            "If not specified, all file types are monitored."
    )
    protected Property<List<String>> mimeTypes;

    @Schema(
        title = "Filter by file owner email address",
        description = "Only files owned by this email address will trigger events"
    )
    protected Property<String> ownerEmail;

    @Schema(
        title = "Whether to include files in subfolders",
        description = "If true, recursively monitors all subfolders. Default is false."
    )
    @Builder.Default
    protected Property<Boolean> includeSubfolders = Property.ofValue(false);

    @Schema(
        title = "The polling interval",
        description = "How frequently to check for new files. Must be at least PT1M (1 minute)."
    )
    @Builder.Default
    protected Property<String> interval = Property.ofValue("PT5M");

    @Schema(
        title = "Maximum number of files to process per poll",
        description = "Limits the number of new files processed in a single poll to avoid overwhelming the system"
    )
    @Builder.Default
    protected Property<Integer> maxFilesPerPoll = Property.ofValue(100);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        // Create Drive connection
        Drive driveService = DriveService.from(runContext, this.serviceAccount);

        Instant lastCreatedTime = context.getNextExecutionDate() != null
            ? context.getNextExecutionDate().toInstant().minus((TemporalAmount) this.interval)
            : Instant.now().minus((TemporalAmount) this.interval);;

        // Build query
        String query = buildQuery(runContext, String.valueOf(lastCreatedTime));
        logger.debug("Executing Drive query: {}", query);

        // Fetch files
        FileList result = driveService.files().list()
            .setQ(query)
            .setPageSize(runContext.render(maxFilesPerPoll).as(Integer.class).orElse(100))
            .setOrderBy("createdTime")
            .setFields("files(id,name,mimeType,createdTime,modifiedTime,owners,parents,size,webViewLink,iconLink,thumbnailLink)")
            .execute();

        List<File> files = result.getFiles();

        if (files == null || files.isEmpty()) {
            logger.debug("No new files found");
            return Optional.empty();
        }

        logger.info("Found {} new file(s)", files.size());

        // Update state with the latest createdTime
        String newLastCreatedTime = files.stream()
            .map(File::getCreatedTime)
            .filter(Objects::nonNull)
            .map(DateTime::toStringRfc3339)
            .max(String::compareTo)
            .orElse(String.valueOf(lastCreatedTime));

        Map<String, Object> newState = new HashMap<>();
        newState.put("lastCreatedTime", newLastCreatedTime);

        // Process first file and create execution
        File firstFile = files.getFirst();
        Output output = Output.builder()
            .id(firstFile.getId())
            .name(firstFile.getName())
            .mimeType(firstFile.getMimeType())
            .createdTime(parseDateTime(firstFile.getCreatedTime()))
            .modifiedTime(parseDateTime(firstFile.getModifiedTime()))
            .owners(firstFile.getOwners())
            .parents(firstFile.getParents())
            .size(firstFile.getSize())
            .webViewLink(firstFile.getWebViewLink())
            .iconLink(firstFile.getIconLink())
            .thumbnailLink(firstFile.getThumbnailLink())
            .build();

        Execution execution = Execution.builder()
                .id(runContext.getTriggerExecutionId())
                    .namespace(context.getNamespace())
                        .flowId(context.getFlowId())
                            .state(new State())
                                .trigger(ExecutionTrigger.of(this,output))
                                    .build();

        logger.info("Triggering execution for file: {} ({})", firstFile.getName(), firstFile.getId());

        return Optional.of(execution);
    }

    private String buildQuery(RunContext runContext, String lastCreatedTime) throws Exception {
        List<String> queryParts = new ArrayList<>();

        // Exclude trashed files
        queryParts.add("trashed = false");

        // Filter by folder
        var renderedFolderId = runContext.render(folderId).as(String.class);
        renderedFolderId.ifPresent(s -> queryParts.add("'" + s + "' in parents"));

        // Filter by MIME type
        var renderedMimeTypes = runContext.render(mimeTypes).asList(String.class);
        if (!renderedMimeTypes.isEmpty()) {
            String mimeTypeQuery = renderedMimeTypes.stream()
                .map(mt -> "mimeType = '" + mt + "'")
                .collect(Collectors.joining(" or "));
            queryParts.add("(" + mimeTypeQuery + ")");
        }

        // Filter by owner
        var renderedOwnerEmail = runContext.render(ownerEmail).as(String.class);
        renderedOwnerEmail.ifPresent(s -> queryParts.add("'" + s + "' in owners"));

        // Filter by creation time (only get files created after last check)
        if (lastCreatedTime != null) {
            queryParts.add("createdTime > '" + lastCreatedTime + "'");
        }

        return String.join(" and ", queryParts);
    }

    private ZonedDateTime parseDateTime(com.google.api.client.util.DateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        return ZonedDateTime.ofInstant(
            Instant.ofEpochMilli(dateTime.getValue()),
            ZoneId.systemDefault()
        );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The file ID"
        )
        private String id;

        @Schema(
            title = "The name of the file"
        )
        private String name;

        @Schema(
            title = "The MIME type of the file"
        )
        private String mimeType;

        @Schema(
            title = "The time the file was created"
        )
        private ZonedDateTime createdTime;

        @Schema(
            title = "The time the file was last modified"
        )
        private ZonedDateTime modifiedTime;

        @Schema(
            title = "The owners of the file"
        )
        private List<User> owners;

        @Schema(
            title = "The parent folder IDs"
        )
        private List<String> parents;

        @Schema(
            title = "The size of the file in bytes"
        )
        private Long size;

        @Schema(
            title = "A link for opening the file in a browser"
        )
        private String webViewLink;

        @Schema(
            title = "A link to the file's icon"
        )
        private String iconLink;

        @Schema(
            title = "A link to the file's thumbnail"
        )
        private String thumbnailLink;
    }
}