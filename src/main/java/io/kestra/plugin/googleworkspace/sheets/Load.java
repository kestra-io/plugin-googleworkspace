package io.kestra.plugin.googleworkspace.sheets;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.SpreadsheetProperties;
import com.google.api.services.sheets.v4.model.UpdateValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Load data from local file to Google Workspace"
)
@Plugin(
	examples = {
		@Example(
			title = "Load data into Google Workspace spreadsheet from an input file",
			code = {
				"type: io.kestra.plugin.googleworkspace.sheets.Load",
				"from: \"{{ inputs.file }}\"",
				"spreadsheetId: xxxxxxxxxxxxxxxxx",
				"range: Sheet2",
				"serviceAccount: \"{{ inputs.serviceAccount }}\"",
				"csvOptions:",
				"  fieldDelimiter: \";\""
			}
		)
	}
)
public class Load extends AbstractLoad implements RunnableTask<Load.Output> {

	@Schema(
		title = "The fully-qualified URIs that point to source data"
	)
	@PluginProperty(dynamic = true)
	private String from;

	@Schema(
		title = "The sheet name or range to select"
	)
	@Builder.Default
	@PluginProperty(dynamic = true)
	private String range = "Sheet1";

	@Override
	public Output run(RunContext runContext) throws Exception {
		Sheets service = this.connection(runContext);
        Logger logger = runContext.logger();

		List<List<Object>> values = this.parse(runContext, from);

		ValueRange body = new ValueRange().setValues(values);

		UpdateValuesResponse response = service.spreadsheets().values()
			.update(runContext.render(this.spreadsheetId), runContext.render(this.range), body)
			.setValueInputOption("RAW")
			.execute();

        logger.debug("Rows updated '{}'", response.getUpdatedRows());

		return Output.builder()
			.range(response.getUpdatedRange())
			.rows(response.getUpdatedRows())
			.columns(response.getUpdatedColumns())
			.build();
	}

	private Spreadsheet createSpreadsheet(Sheets service) throws IOException {
		SpreadsheetProperties properties = new SpreadsheetProperties()
			.setTitle("New Spreadsheet");

		Spreadsheet spreadsheet = new Spreadsheet()
			.setProperties(properties);

		return service
			.spreadsheets()
			.create(spreadsheet)
			.execute();
	}

	@Getter
	@Builder
	public static class Output implements io.kestra.core.models.tasks.Output {

		@Schema(
			title = "The size of the rows fetch"
		)
		private String range;

		@Schema(
			title = "The size of the rows fetch"
		)
		private int rows;

		@Schema(
			title = "The size of the rows fetch"
		)
		private int columns;

	}

}
