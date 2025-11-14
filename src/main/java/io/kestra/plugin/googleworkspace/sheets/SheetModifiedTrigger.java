package io.kestra.plugin.googleworkspace.sheets;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.Revision;
import com.google.api.services.drive.model.RevisionList;
import com.google.api.services.sheets.v4.Sheets;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode
@ToString
@Getter
@Schema(

)
public class SheetModifiedTrigger extends AbstractTrigger implements TriggerOutput<SheetModifiedTrigger.Output>, StatefulTriggerInterface {

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
        title = "Read timeout",
        description = "Read timeout for API requests in seconds"
    )
    @Builder.Default
    private Property<Integer> readTimeout = Property.ofValue(120);

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
        List<String> rScopes = runContext.render(scopes).asList(String.class).orElse(Arrays.asList(
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.metadata.readonly"
        ));
        Integer rReadTimeout = runContext.render(readTimeout).as(Integer.class).orElse(120);

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

        


    }
}
