package io.kestra.plugin.googleworkspace.sheets;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.UpdateValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
    title = "Load data from a local file to a Google Workspace spreadsheet"
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

	@Schema(
		title = "The URI of the Kestra's internal storage file."
	)
	private Property<String> from;

	@Schema(
		title = "The sheet name or range to select."
	)
	@Builder.Default
	private Property<String> range = Property.of("Sheet1");

	@Override
	public Output run(RunContext runContext) throws Exception {
		Sheets service = this.connection(runContext);
        Logger logger = runContext.logger();

        URI from = URI.create(runContext.render(this.from).as(String.class).orElseThrow());
		List<List<Object>> values = this.parse(runContext, from);

		ValueRange body = new ValueRange().setValues(values);

		UpdateValuesResponse response = service.spreadsheets().values()
			.update(runContext.render(this.spreadsheetId).as(String.class).orElseThrow(),
                runContext.render(this.range).as(String.class).orElseThrow(),
                body)
			.setValueInputOption("RAW")
			.execute();

        logger.debug("Rows updated '{}'", response.getUpdatedRows());

		return Output.builder()
			.range(response.getUpdatedRange())
			.rows(response.getUpdatedRows())
			.columns(response.getUpdatedColumns())
			.build();
	}

	@Getter
	@Builder
	public static class Output implements io.kestra.core.models.tasks.Output {

		@Schema(
			title = "The spreadsheet ID or range"
		)
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
