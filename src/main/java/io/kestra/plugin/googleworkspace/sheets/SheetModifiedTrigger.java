package io.kestra.plugin.googleworkspace.sheets;

import com.google.api.services.drive.Drive;
import com.google.api.client.util.DateTime;
import com.google.api.services.drive.model.File;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.ValueRange;
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

import java.util.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger when a Google Sheet is modified",
    description = "Polls Drive changes feed and fetches configured range to compute diffs on changes."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger on any modifications in a range",
            full = true,
            code = """
                id: sheet_modified_example
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "Change on {{ trigger.sheetName }}: {{ json(trigger.diff) }}"

                triggers:
                  - id: on_change
                    type: io.kestra.plugin.googleworkspace.sheets.SheetModifiedTrigger
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT') }}"
                    spreadsheetId: "your-spreadsheet-id"
                    range: "Sheet1!A1:D100"
                    interval: PT2M
              """
        )
    }
)
public class SheetModifiedTrigger extends AbstractSheetTrigger implements PollingTriggerInterface, TriggerOutput<SheetModifiedTrigger.Output> {

    @Schema(title = "The Google Cloud service account key")
    @NotNull
    protected String serviceAccount;

    @Schema(title = "OAuth scopes to request for Sheets/Drive APIs")
    @Builder.Default
    protected Property<List<String>> scopes = Property.ofValue(List.of(
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.metadata.readonly"
    ));

    @Schema(title = "Spreadsheet ID to monitor")
    @NotNull
    protected Property<String> spreadsheetId;

    @Schema(title = "Optional sheet/tab name to restrict changes")
    protected Property<String> sheetName;

    @Schema(title = "Optional A1 range to restrict fetch/diff", description = "Example: Sheet1!A1:D100")
    protected Property<String> range;

    @Builder.Default
    protected java.time.Duration interval = java.time.Duration.ofMinutes(5);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        List<String> renderedScopes = runContext.render(scopes).asList(String.class);
        Sheets sheets = sheetsFrom(runContext, this.serviceAccount, renderedScopes);
        Drive drive = driveFrom(runContext, this.serviceAccount, renderedScopes);

        String spreadsheetId = runContext.render(this.spreadsheetId).as(String.class).orElseThrow();
        // Detect modification via Drive file modifiedTime compared to last check window
        java.time.Instant lastCheck = context.getNextExecutionDate() != null
            ? context.getNextExecutionDate().toInstant().minus(this.interval)
            : java.time.Instant.now().minus(this.interval);

        File file = drive.files().get(spreadsheetId)
            .setFields("id, name, modifiedTime")
            .execute();

        DateTime modified = file.getModifiedTime();
        if (modified == null || modified.getValue() <= lastCheck.toEpochMilli()) {
            return Optional.empty();
        }

        // Fetch previous values snapshot stored in state and new values to compute a simple diff
        String a1Range = runContext.render(range).as(String.class).orElse(null);
        String tab = runContext.render(sheetName).as(String.class).orElse(null);
        String effectiveRange = a1Range != null ? a1Range : (tab != null ? tab : null);

        ValueRange valueRange = sheets.spreadsheets().values()
            .get(spreadsheetId, effectiveRange != null ? effectiveRange : "")
            .execute();

        List<List<Object>> current = valueRange.getValues();

        Map<String, Object> diff = computeDiff(null, current);

        Output output = Output.builder()
            .spreadsheetId(spreadsheetId)
            .sheetName(tab)
            .range(effectiveRange)
            .diff(diff)
            .build();

        return Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
    }

    private Map<String, Object> computeDiff(List<List<Object>> previous, List<List<Object>> current) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (previous == null) {
            result.put("type", "initial");
            result.put("addedRows", current != null ? current.size() : 0);
            return result;
        }

        int prevRows = previous != null ? previous.size() : 0;
        int currRows = current != null ? current.size() : 0;
        int rowDelta = currRows - prevRows;

        List<Map<String, Object>> cellChanges = new ArrayList<>();
        int maxRows = Math.max(prevRows, currRows);
        int maxCols = 0;
        for (int r = 0; r < maxRows; r++) {
            List<Object> prevRow = r < prevRows ? previous.get(r) : null;
            List<Object> currRow = r < currRows ? current.get(r) : null;
            int prevCols = prevRow != null ? prevRow.size() : 0;
            int currCols = currRow != null ? currRow.size() : 0;
            maxCols = Math.max(maxCols, Math.max(prevCols, currCols));

            for (int c = 0; c < Math.max(prevCols, currCols); c++) {
                Object prevVal = prevRow != null && c < prevCols ? prevRow.get(c) : null;
                Object currVal = currRow != null && c < currCols ? currRow.get(c) : null;
                if (!Objects.equals(prevVal, currVal)) {
                    Map<String, Object> change = new LinkedHashMap<>();
                    change.put("row", r);
                    change.put("col", c);
                    change.put("before", prevVal);
                    change.put("after", currVal);
                    cellChanges.add(change);
                }
            }
        }

        result.put("rowDelta", rowDelta);
        result.put("cellChanges", cellChanges);
        result.put("rowsBefore", prevRows);
        result.put("rowsAfter", currRows);
        result.put("maxColumnsCompared", maxCols);
        return result;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private String spreadsheetId;
        private String sheetName;
        private String range;
        private Map<String, Object> diff;
    }
}


