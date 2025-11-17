package io.kestra.plugin.googleworkspace.sheets;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.Revision;
import com.google.api.services.drive.model.RevisionList;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
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

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.kestra.core.models.triggers.StatefulTriggerService.*;
import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode
@ToString
@Getter
@Schema(
    title = "Trigger on Google Sheet modifications",
    description = """
        Polls a Google Sheet at regular intervals and fires when content changes are detected.

        Changes are tracked using Google Drive's revision system. The trigger detects:
        - Cell value modifications
        - Row/column additions or deletions
        - Sheet tab additions or removals

        Optionally filter by specific sheet names or cell ranges.

        **Authentication**: Requires a GCP service account with both
        Sheets API (spreadsheets.readonly) and Drive API (drive.metadata.readonly) access.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Monitor entire spreadsheet for changes",
            full = true,
            code = """
                id: monitor_spreadsheet
                namespace: company.team

                tasks:
                  - id: log_changes
                    type: io.kestra.plugin.core.log.Log
                    message: "Sheet '{{ trigger.sheetName }}' was modified: {{ trigger.modifications }}"

                triggers:
                  - id: watch_sheet
                    type: io.kestra.plugin.googleworkspace.sheets.SheetModifiedTrigger
                    interval: PT5M
                    spreadsheetId: "1U4AoiUrqiVaSIVcm_TwDc9RoKOdCULNGWxuC1vmDT_A"
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"
                """
        ),
        @Example(
            title = "Monitor specific sheet tab with range filter",
            full = true,
            code = """
                id: monitor_orders_sheet
                namespace: company.team

                tasks:
                  - id: process_changes
                    type: io.kestra.plugin.core.debug.Return
                    format: |
                      Changes detected:
                      - Modified at: {{ trigger.modifiedTime }}
                      - Revision: {{ trigger.revisionId }}
                      - Changed rows: {{ trigger.changedRows }}

                triggers:
                  - id: watch_orders
                    type: io.kestra.plugin.googleworkspace.sheets.SheetModifiedTrigger
                    interval: PT2M
                    spreadsheetId: "1U4AoiUrqiVaSIVcm_TwDc9RoKOdCULNGWxuC1vmDT_A"
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"
                    sheetName: "Orders"
                    range: "A1:F100"
                    stateTtl: P7D
                """
        ),
        @Example(
            title = "Track only new revisions (CREATE mode)",
            full = true,
            code = """
                id: track_new_revisions
                namespace: company.team

                tasks:
                  - id: notify
                    type: io.kestra.plugin.notifications.slack.SlackIncomingWebhook
                    url: "{{ secret('SLACK_WEBHOOK') }}"
                    payload: |
                      {
                        "text": "Spreadsheet '{{ trigger.spreadsheetTitle }}' was modified by {{ trigger.lastModifyingUser }}"
                      }

                triggers:
                  - id: watch
                    type: io.kestra.plugin.googleworkspace.sheets.SheetModifiedTrigger
                    interval: PT1M
                    spreadsheetId: "{{ vars.spreadsheet_id }}"
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"
                    on: CREATE
                    includeDetails: true
                """
        )
    }

)
public class SheetModifiedTrigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<SheetModifiedTrigger.Output>, StatefulTriggerInterface {

    @Builder.Default
    @Schema(
        title = "Polling interval",
        description = "How often to check for sheet modifications"
    )
    private final Duration interval = Duration.ofMinutes(5);

    @Schema(
        title = "Spreadsheet ID",
        description = "The unique identifier of the Google Spreadsheet. " +
            "Found in the URL: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit"
    )
    @NotNull
    private Property<String> spreadsheetId;

    @Schema(
        title = "Service Account Json",
        description = "GCP service account key in JSON format. "
    )
    @NotNull
    private Property<String> serviceAccount;

    @Schema(
        title = "Sheet name filter",
        description = "Optional sheet (tab) name to monitor. If not specified, monitors all sheets."
    )
    private Property<String> sheetName;

    @Schema(
        title = "Range filter",
        description = "Optional A1 notation range (e.g., 'A1:D10'). Only monitors changes within this range."
    )
    private Property<String> range;

    @Schema(
        title = "Include change details",
        description = "If true, fetches full sheet content to compute detailed diff."
    )
    @Builder.Default
    private Property<Boolean> includeDetails = Property.ofValue(false);

    @Schema(
        title = "API scopes",
        description = "Google API OAuth scopes"
    )
    @Builder.Default
    private Property<List<String>> scopes = Property.ofValue(Arrays.asList(
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.metadata.readonly"
    ));

    @Schema(
        title = "State key",
        description = "Custom key for state storage. Defaults to trigger ID."
    )
    private Property<String> stateKey;

    @Schema(
        title = "State TTL",
        description = "How long to remember processed revisions (e.g., P7D for 7 days)"
    )
    private Property<Duration> stateTtl;

    @Schema(
        title = "Trigger on",
        description = """
            When to fire the trigger:
            - CREATE: Only for newly detected revisions
            - UPDATE: Only when revision changes (not typical for sheets)
            - CREATE_OR_UPDATE: On any new or changed revision (recommended)
            """
    )
    @Builder.Default
    private Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {

        var runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        String rSpreadsheetId = runContext.render(spreadsheetId).as(String.class).orElseThrow();
        String rServiceAccount = runContext.render(serviceAccount).as(String.class).orElseThrow();
        String rSheetName = sheetName != null ? runContext.render(sheetName).as(String.class).orElse(null) : null;
        String rRange = range != null ? runContext.render(range).as(String.class).orElse(null) : null;
        Boolean rIncludeDetails = runContext.render(includeDetails).as(Boolean.class).orElse(false);
        List<String> rScopes = runContext.render(scopes).asList(String.class);

        logger.debug("Checking spreadsheet {} for modification", rSpreadsheetId);

        GoogleCredentials credentials = GoogleCredentials.fromStream(
            new ByteArrayInputStream(rServiceAccount.getBytes())
        )
            .createScoped(rScopes);

        Drive drive = new Drive.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(credentials)
        )
            .setApplicationName("kestra")
            .build();

        Sheets sheets = new Sheets.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(credentials)
        )
            .setApplicationName("kestra")
            .build();

        // Fetch revisions from drive API
        List<Revision> revisions;
        try {
            RevisionList revisionList = drive.revisions()
                .list(rSpreadsheetId)
                .setFields("revisions(id,modifiedTime,lastModifyingUser)")
                .execute();

            revisions = revisionList.getRevisions();
        } catch (Exception e) {
            logger.warn("Failed to fetch revisions: {}", e.getMessage());
            return Optional.empty();
        }

        if (revisions == null || revisions.isEmpty()) {
            logger.debug("No revisions found");
            return Optional.empty();
        }

        logger.info("Found {} revision(s) for spreadsheet", revisions.size());

        var rStateKey = runContext.render(stateKey).as(String.class).orElse(defaultKey(context.getNamespace(), context.getFlowId(), id));
        var rStateTtl = runContext.render(stateTtl).as(Duration.class);

        Map<String, Entry> state = readState(runContext, rStateKey, rStateTtl);

        List<ModificationOutput> toFire = revisions.stream()
            .flatMap(throwFunction(revision -> {
                String revisionUri = String.format("sheet://%s/revision/%s",rSpreadsheetId, revision.getId());

                String version = revision.getId();

                Instant modifiedAt = revision.getModifiedTime() != null ? Instant.ofEpochMilli(revision.getModifiedTime().getValue()) : Instant.now();

                var candidate = Entry.candidate(revisionUri, version, modifiedAt);
                var update = computeAndUpdateState(state, candidate, runContext.render(getOn()).as(On.class).orElse(On.CREATE));

                if (update.fire()) {
                    logger.debug("New revision detected: {}", revision.getId());

                    Spreadsheet spreadsheet = sheets.spreadsheets()
                        .get(rSpreadsheetId)
                        .execute();

                    ModificationOutput.ModificationOutputBuilder outputBuilder = ModificationOutput.builder()
                        .revisionId(revision.getId())
                        .modifiedTime(modifiedAt)
                        .spreadsheetTitle(spreadsheet.getProperties().getTitle())
                        .spreadsheetId(rSpreadsheetId)
                        .lastModifyingUser(revision.getLastModifyingUser() != null ? revision.getLastModifyingUser().getDisplayName() : null);

                    if (rSheetName != null) {
                        Sheet targetSheet = spreadsheet.getSheets().stream()
                            .filter(s -> s.getProperties().getTitle().equals(rSheetName))
                            .findFirst()
                            .orElse(null);

                        if (targetSheet == null) {
                            logger.warn("Sheet '{}' not found in spreadsheet", rSheetName);
                            return Stream.empty();
                        }

                        outputBuilder.sheetName(rSheetName);
                    }

                    if (rIncludeDetails) {
                        try {
                            ChangeDetails details = fetchChangeDetails(
                                sheets,
                                rSpreadsheetId,
                                rSheetName,
                                rRange,
                                runContext
                            );
                            outputBuilder.changeDetails(details);
                        } catch (Exception e) {
                            logger.warn("Failed to fetch change details: {}", e.getMessage());
                        }
                    }

                    return Stream.of(outputBuilder.build());
                }
                return Stream.empty();
            }))
            .toList();

        writeState(runContext, rStateKey, state, rStateTtl);

        if (toFire.isEmpty()) {
            logger.debug("No new modifications detected after state evaluation");
        }

        logger.info("Triggering flow with {} modification(s)", toFire.size());

        var output = Output.builder()
            .modifications(toFire)
            .count(toFire.size())
            .build();

        return Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
    }

    private ChangeDetails fetchChangeDetails(Sheets sheets, String rSpreadsheetId, String finalRSheetName, String finalRange, RunContext runContext) throws Exception{
        String rangeSpec = finalRSheetName != null ?
            (finalRange != null ? finalRSheetName + "!" + finalRange : finalRSheetName) :
            finalRange;

        var res = sheets.spreadsheets().values()
            .get(rSpreadsheetId, rangeSpec != null ? rangeSpec : "")
            .execute();

        List<List<Object>> values = res.getValues();

        int rowCount = values != null ? values.size() : 0;
        int maxCols = values != null ? values.stream()
            .mapToInt(List::size)
            .max()
            .orElse(0) : 0;

        return ChangeDetails.builder()
            .affectedRange(res.getRange())
            .rowCount(rowCount)
            .columnCount(maxCols)
            .hasData(rowCount > 0)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List of sheet modifications",
            description = "New revisions detected in this polling cycle"
        )
        private final List<ModificationOutput> modifications;

        @Schema(
            title = "Number of modifications",
            description = "Count of new revisions detected"
        )
        private final Integer count;
    }

    @Builder
    @Getter
    public static class ModificationOutput {
        @Schema(
            title = "Revision ID",
            description = "Google Drive revision identifier"
        )
        private final String revisionId;

        @Schema(
            title = "Modified time",
            description = "When this revision was created"
        )
        private final Instant modifiedTime;

        @Schema(
            title = "Spreadsheet title",
            description = "Name of the spreadsheet"
        )
        private final String spreadsheetTitle;

        @Schema(
            title = "Spreadsheet ID",
            description = "Unique spreadsheet identifier"
        )
        private final String spreadsheetId;

        @Schema(
            title = "Last modifying user",
            description = "Display name of the user who made the change"
        )
        private final String lastModifyingUser;

        @Schema(
            title = "Sheet name",
            description = "Name of the modified sheet (tab), if filter was applied"
        )
        private final String sheetName;

        @Schema(
            title = "Change details",
            description = "Detailed change information (if includeDetails=true)"
        )
        private final ChangeDetails changeDetails;
    }

    @Builder
    @Getter
    public static class ChangeDetails {
        @Schema(
            title = "Affected range",
            description = "A1 notation of the range that was checked"
        )
        private final String affectedRange;

        @Schema(
            title = "Row count",
            description = "Current number of rows in the range"
        )
        private final Integer rowCount;

        @Schema(
            title = "Column count",
            description = "Current number of columns in the range"
        )
        private final Integer columnCount;

        @Schema(
            title = "Has data",
            description = "Whether the range contains any data"
        )
        private final Boolean hasData;
    }
}
