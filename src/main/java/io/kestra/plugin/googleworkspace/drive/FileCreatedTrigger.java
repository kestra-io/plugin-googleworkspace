package io.kestra.plugin.googleworkspace.drive;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.User;
import com.google.common.collect.Lists;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

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
                    type: io.kestra.plugin.slack.notifications.SlackIncomingWebhook
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

    // Drive can reject long OR chains as "query is too complex", so each file query covers at most this many folders
    private static final int MAX_FOLDERS_PER_QUERY = 50;

    @Schema(
        title = "OAuth scopes",
        description = "Drive scopes to request; default metadata read-only"
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<List<String>> scopes = Property.ofValue(List.of("https://www.googleapis.com/auth/drive.metadata.readonly"));

    @Schema(
        title = "Folder ID to monitor",
        description = """
            Google Drive folder ID. The entire Drive is monitored if omitted.
            """
    )
    @PluginProperty(group = "advanced")
    protected Property<String> folderId;

    @Schema(
        title = "MIME type filters",
        description = "Restrict to listed MIME types (e.g., application/pdf). Empty means all types."
    )
    @PluginProperty(group = "advanced")
    protected Property<List<String>> mimeTypes;

    @Schema(
        title = "Owner email filter",
        description = "Only files owned by this email trigger executions; must be a valid email"
    )
    @PluginProperty(group = "advanced")
    protected Property<String> ownerEmail;

    @Schema(
        title = "Include subfolders",
        description = """
            When true, files created in any subfolder of `folderId`, at any depth, also trigger an execution. Ignored when `folderId` is not set.
            Each poll lists every subfolder, so a large tree costs one Drive call per folder per poll. Default false.
            """
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Boolean> includeSubfolders = Property.ofValue(false);

    @Schema(
        title = "Polling interval",
        description = "How often to poll for new files; minimum PT1M, default PT1H"
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Duration interval = Duration.ofHours(1);

    @Schema(
        title = "Files processed per poll",
        description = "Upper bound on files returned each poll; default 100"
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Integer> maxFilesPerPoll = Property.ofValue(100);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        var driveService = from(runContext);

        var lastCreatedTime = context.getNextExecutionDate() != null
            ? context.getNextExecutionDate().toInstant().minus((TemporalAmount) this.interval)
            : Instant.now().minus((TemporalAmount) this.interval);

        var rMaxFilesPerPoll = runContext.render(maxFilesPerPoll).as(Integer.class).orElse(100);

        // A file with parents in two chunks comes back from both queries, so results are merged by id
        var filesById = new LinkedHashMap<String, File>();
        for (var query : buildQueries(runContext, String.valueOf(lastCreatedTime), driveService)) {
            logger.debug("Executing Drive query: {}", query);

            var result = driveService.files().list()
                .setQ(query)
                .setPageSize(rMaxFilesPerPoll)
                .setOrderBy("createdTime")
                .setFields("files(id,name,mimeType,createdTime,modifiedTime,owners,parents,size,webViewLink,iconLink,thumbnailLink)")
                .execute();

            if (result.getFiles() != null) {
                result.getFiles().forEach(file -> filesById.putIfAbsent(file.getId(), file));
            }
        }

        // Each query returns its own oldest files, so the merged files are sorted again before applying maxFilesPerPoll
        var files = filesById.values().stream()
            .sorted(Comparator.comparing(File::getCreatedTime, Comparator.nullsLast(Comparator.comparingLong(DateTime::getValue))))
            .limit(rMaxFilesPerPoll)
            .toList();

        if (files.isEmpty()) {
            logger.debug("No new files found");
            return Optional.empty();
        }

        logger.info("Found {} new file(s)", files.size());

        // Update state with the latest createdTime
        var newLastCreatedTime = files.stream()
            .map(File::getCreatedTime)
            .filter(Objects::nonNull)
            .map(DateTime::toStringRfc3339)
            .max(String::compareTo)
            .orElse(String.valueOf(lastCreatedTime));

        var newState = new HashMap<String, Object>();
        newState.put("lastCreatedTime", newLastCreatedTime);

        // Process first file and create execution
        var outputFiles = new ArrayList<Output.FileMetadata>();

        for (var file : files) {
            URI kestraUri = null;
            try {
                var fileContent = driveService.files()
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

        var output = Output.builder()
            .files(outputFiles)
            .build();

        var execution = TriggerService.generateExecution(this, conditionContext, context, output);

        logger.info("Triggering execution with {} new files", outputFiles.size());

        return Optional.of(execution);
    }

    // Package-private so the queries can be tested without a Drive account
    List<String> buildQueries(RunContext runContext, String lastCreatedTime, Drive drive) throws Exception {
        var queryParts = new ArrayList<String>();

        // Exclude trashed files
        queryParts.add("trashed = false");

        // Filter by MIME type
        var rMimeTypes = runContext.render(mimeTypes).asList(String.class);
        if (!rMimeTypes.isEmpty()) {
            var mimeTypeQuery = rMimeTypes.stream()
                .map("mimeType = '%s'"::formatted)
                .collect(Collectors.joining(" or "));
            queryParts.add("(%s)".formatted(mimeTypeQuery));
        }

        // Filter by owner
        var rOwnerEmail = runContext.render(ownerEmail).as(String.class);
        if (rOwnerEmail.isPresent()) {
            var email = rOwnerEmail.get();
            if (!email.matches("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$")) {
                throw new IllegalArgumentException("Invalid ownerEmail: %s".formatted(email));
            }
            queryParts.add("'%s' in owners".formatted(email));
        }

        // Filter by creation time (only get files created after last check)
        if (lastCreatedTime != null) {
            queryParts.add("createdTime > '%s'".formatted(lastCreatedTime));
        }

        var query = String.join(" and ", queryParts);

        // Filter by folder, optionally including every descendant subfolder
        var rFolderId = runContext.render(folderId).as(String.class);
        if (rFolderId.isEmpty()) {
            return List.of(query);
        }

        var rIncludeSubfolders = runContext.render(includeSubfolders).as(Boolean.class).orElse(false);
        var folderIds = rIncludeSubfolders ? collectFolderIds(drive, rFolderId.get()) : List.of(rFolderId.get());

        var queries = new ArrayList<String>();
        for (var chunk : Lists.partition(folderIds, MAX_FOLDERS_PER_QUERY)) {
            var parentClause = chunk.stream()
                .map("'%s' in parents"::formatted)
                .collect(Collectors.joining(" or "));
            queries.add("%s and %s".formatted(query, chunk.size() > 1 ? "(%s)".formatted(parentClause) : parentClause));
        }
        return queries;
    }

    // Breadth-first walk returning the root folder id and every descendant folder id, each once
    private List<String> collectFolderIds(Drive drive, String rootFolderId) throws Exception {
        var visited = new LinkedHashSet<String>();
        visited.add(rootFolderId);

        var queue = new ArrayDeque<String>();
        queue.add(rootFolderId);

        while (!queue.isEmpty()) {
            var parentId = queue.poll();
            String pageToken = null;
            do {
                var result = drive.files().list()
                    .setQ("mimeType = 'application/vnd.google-apps.folder' and '%s' in parents and trashed = false".formatted(parentId))
                    .setFields("nextPageToken, files(id)")
                    .setPageSize(1000)
                    .setPageToken(pageToken)
                    .execute();
                if (result.getFiles() != null) {
                    for (var folder : result.getFiles()) {
                        // A folder with several parents is listed under each of them, so it is only queued the first time
                        if (visited.add(folder.getId())) {
                            queue.add(folder.getId());
                        }
                    }
                }
                pageToken = result.getNextPageToken();
            } while (pageToken != null);
        }

        return List.copyOf(visited);
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
