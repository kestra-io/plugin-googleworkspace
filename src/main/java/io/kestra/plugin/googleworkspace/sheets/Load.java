package io.kestra.plugin.googleworkspace.sheets;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.AppendValuesResponse;
import com.google.api.services.sheets.v4.model.ClearValuesRequest;
import com.google.api.services.sheets.v4.model.UpdateValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.net.URI;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Load data from a local file to a Google Sheet."
)
@Plugin(
	examples = {
		@Example(
			title = "Load data into a Google Workspace spreadsheet from an input file",
			full = true,
			code = """
			    id: googleworkspace_sheets_load
				namespace: company.team

				inputs:
				  - id: file
				    type: FILE
				  - id: serviceAccount
				    type: STRING

				tasks:
				  - id: load_data
				    type: io.kestra.plugin.googleworkspace.sheets.Load
				    from: "{{ inputs.file }}"
				    spreadsheetId: xxxxxxxxxxxxxxxxx
				    range: Sheet2
				    serviceAccount: "{{ inputs.serviceAccount }}"
				    csvOptions:
				      fieldDelimiter: ";"
			    """
		)
	}
)
public class Load extends AbstractLoad implements RunnableTask<Load.Output> {
    private static final String VALUE_INPUT_OPTION = "RAW";

	@Schema(
		title = "The URI of the Kestra's internal storage file"
	)
	private Property<String> from;

	@Schema(
		title = "The sheet name or range to select"
	)
	@Builder.Default
	private Property<String> range = Property.ofValue("Sheet1");

    @Schema(
        title = "How to write the data into the sheet",
        description = """
            UPDATE (default): write values to the given range; does not clear extra old data outside the written area.
            OVERWRITE: clear the target range first, then write values.
            APPEND: append values as new rows after the last non-empty row in the given sheet/range.
            """
    )
    @Builder.Default
    private Property<InsertType> insertType = Property.ofValue(InsertType.UPDATE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Sheets service = this.connection(runContext);
        Logger logger = runContext.logger();

        URI from = URI.create(runContext.render(this.from).as(String.class).orElseThrow());
		List<List<Object>> values = this.parse(runContext, from);

        String rSpreadsheetId = runContext.render(this.spreadsheetId).as(String.class).orElseThrow();
        String rTargetRange = runContext.render(this.range).as(String.class).orElseThrow();
        InsertType rMode = runContext.render(this.insertType).as(InsertType.class).orElse(InsertType.UPDATE);

        ValueRange body = new ValueRange().setValues(values);

        WriteResult r = executeWrite(service, rSpreadsheetId, rTargetRange, body, rMode);
        logger.debug("Rows updated '{}', columns '{}', range '{}'", r.rows(), r.columns(), r.range());

        return Output.builder()
            .range(r.range())
            .rows(r.rows())
            .columns(r.columns())
            .build();
    }

    private static WriteResult executeWrite(Sheets service, String spreadsheetId, String range, ValueRange body, InsertType mode) throws Exception {
        return switch (mode) {
            case OVERWRITE -> {
                service.spreadsheets().values()
                    .clear(spreadsheetId, range, new ClearValuesRequest())
                    .execute();

                UpdateValuesResponse resp = service.spreadsheets().values()
                    .update(spreadsheetId, range, body)
                    .setValueInputOption(VALUE_INPUT_OPTION)
                    .execute();

                yield new WriteResult(
                    resp.getUpdatedRange(),
                    resp.getUpdatedRows() == null ? 0 : resp.getUpdatedRows(),
                    resp.getUpdatedColumns() == null ? 0 : resp.getUpdatedColumns()
                );
            }
            case APPEND -> {
                AppendValuesResponse resp = service.spreadsheets().values()
                    .append(spreadsheetId, range, body)
                    .setValueInputOption(VALUE_INPUT_OPTION)
                    .setInsertDataOption("INSERT_ROWS")
                    .setIncludeValuesInResponse(false)
                    .execute();

                var updates = resp.getUpdates();
                yield new WriteResult(
                    updates == null ? range : updates.getUpdatedRange(),
                    (updates == null || updates.getUpdatedRows() == null) ? 0 : updates.getUpdatedRows(),
                    (updates == null || updates.getUpdatedColumns() == null) ? 0 : updates.getUpdatedColumns()
                );
            }
            case UPDATE -> {
                UpdateValuesResponse resp = service.spreadsheets().values()
                    .update(spreadsheetId, range, body)
                    .setValueInputOption(VALUE_INPUT_OPTION)
                    .execute();

                yield new WriteResult(
                    resp.getUpdatedRange(),
                    resp.getUpdatedRows() == null ? 0 : resp.getUpdatedRows(),
                    resp.getUpdatedColumns() == null ? 0 : resp.getUpdatedColumns()
                );
            }
        };
    }

    private record WriteResult(String range, int rows, int columns) {}

    @Getter
    @NoArgsConstructor
    public enum InsertType { UPDATE, OVERWRITE, APPEND }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The spreadsheet ID or range")
        private String range;

		@Schema(
			title = "The number of rows loaded"
		)
		private int rows;

		@Schema(
			title = "The number of columns loaded"
		)
		private int columns;

	}

}
