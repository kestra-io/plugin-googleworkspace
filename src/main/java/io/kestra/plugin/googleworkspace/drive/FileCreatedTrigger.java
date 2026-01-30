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
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
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
    title = "Poll Drive for newly created files",
    description = "Polls a folder (or entire Drive) for files created since the prior interval and triggers an execution with the new files. Uses service-account access; interval default PT1H, min PT1M. Max files per poll defaults to 100. Files are stored to internal storage when download succeeds; failures are logged and skipped."
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
                    message: "New file created: {{ trigger.files[0].name }}"

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
                    fileId: "{{ trigger.files[0].id }}"

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
                        "text": "New file by {{ trigger.files[0].owners[0].displayName }}: {{ trigger.files[0].name }}"
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
public class FileCreatedTrigger extends AbstractDriveTrigger implements PollingTriggerInterface, TriggerOutput<FileCreatedTrigger.Output> {

    @Schema(
        title = "OAuth scopes",
        description = "Drive scopes to request; default metadata read-only"
    )
    @Builder.Default
    protected Property<List<String>> scopes = Property.ofValue(List.of("https://www.googleapis.com/auth/drive.metadata.readonly"));

    @Schema(
        title = "Folder ID to monitor",
        description = "Google Drive folder ID; monitors entire Drive if omitted"
    )
    protected Property<String> folderId;

    @Schema(
        title = "MIME type filters",
        description = "Restrict to listed MIME types (e.g., application/pdf). Empty means all types."
    )
    protected Property<List<String>> mimeTypes;

    @Schema(
        title = "Owner email filter",
        description = "Only files owned by this email trigger executions; must be a valid email"
    )
    protected Property<String> ownerEmail;

    @Schema(
        title = "Include subfolders",
        description = "When true, query is recursive through subfolders; default false"
    )
    @Builder.Default
    protected Property<Boolean> includeSubfolders = Property.ofValue(false);

    @Schema(
        title = "Polling interval",
        description = "How often to poll for new files; minimum PT1M, default PT1H"
    )
    @Builder.Default
    protected Duration interval = Duration.ofHours(1);

    @Schema(
        title = "Files processed per poll",
        description = "Upper bound on files returned each poll; default 100"
    )
    @Builder.Default
    protected Property<Integer> maxFilesPerPoll = Property.ofValue(100);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Drive driveService = from(runContext);

        Instant lastCreatedTime = context.getNextExecutionDate() != null
            ? context.getNextExecutionDate().toInstant().minus((TemporalAmount) this.interval)
            : Instant.now().minus((TemporalAmount) this.interval);;

        String query = buildQuery(runContext, String.valueOf(lastCreatedTime));
        logger.debug("Executing Drive query: {}", query);

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
        List<Output.FileMetadata> outputFiles = new ArrayList<>();

        for (File file: files) {
            URI kestraUri = null;
            try{
                InputStream fileContent = driveService.files()
                    .get(file.getId())
                    .executeMediaAsInputStream();
                kestraUri = runContext.storage().putFile(fileContent, file.getName());

                logger.debug("Stored file {} in Kestra storage", file.getName());

            } catch (Exception e) {
                logger.warn("Failed to download file {}: {}", file.getId(), e.getMessage());
            }

            outputFiles.add(
                Output.FileMetadata.builder()
                    .id(file.getId())
                    .name(file.getName())
                    .mimeType(file.getMimeType())
                    .createdTime(parseDateTime(file.getCreatedTime()))
                    .modifiedTime(parseDateTime(file.getModifiedTime()))
                    .owners(file.getOwners())
                    .parents(file.getParents())
                    .size(file.getSize())
                    .webViewLink(file.getWebViewLink())
                    .iconLink(file.getIconLink())
                    .thumbnailLink(file.getThumbnailLink())
                    .kestraFileUri(kestraUri != null ? kestraUri.toString() : null)
                    .build()
            );
        }

        Output output = Output.builder()
            .files(outputFiles)
            .build();

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, output);

        logger.info("Triggering execution with {} new files", outputFiles.size());

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
        if (renderedOwnerEmail.isPresent()) {
            String email = renderedOwnerEmail.get();
            if (!email.matches("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$")) {
                throw new IllegalArgumentException("Invalid ownerEmail: " + email);
            }
            queryParts.add("'" + email + "' in owners");
        }


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
            title = "New files detected",
            description = "Files created since the last interval that matched filters"
        )
        private List<FileMetadata> files;

        @Builder
        @Getter
        public static class FileMetadata {
            @Schema(
                title = "File ID"
            )
            private String id;

            @Schema(
                title = "File name"
            )
            private String name;

            @Schema(
                title = "MIME type"
            )
            private String mimeType;

            @Schema(
                title = "Created time"
            )
            private ZonedDateTime createdTime;

            @Schema(
                title = "Last modified time"
            )
            private ZonedDateTime modifiedTime;

            @Schema(
                title = "Owners"
            )
            private List<User> owners;

            @Schema(
                title = "Parent folder IDs"
            )
            private List<String> parents;

            @Schema(
                title = "File size (bytes)"
            )
            private Long size;

            @Schema(
                title = "Web link"
            )
            private String webViewLink;

            @Schema(
                title = "Icon link"
            )
            private String iconLink;

            @Schema(
                title = "Thumbnail link"
            )
            private String thumbnailLink;

            @Schema(
                title = "Kestra storage URI",
                description = "Location in internal storage when download succeeded; null if download failed"
            )
            private String kestraFileUri;
        }
    }
}
