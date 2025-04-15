package io.kestra.plugin.googleworkspace.sheets;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.SpreadsheetProperties;
import com.google.api.services.sheets.v4.model.UpdateValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
    title = "Create a Google Sheet."
)
@Plugin(
    examples = {
        @Example(
            title = "Create a spreadsheet in Google Workspace",
			full = true,
            code = """
                id: googleworkspace_sheets_create
                namespace: company.team

                inputs:
                  - id: serviceAccount
                    type: STRING

                tasks:
                  - id: create_spreadsheet
                    type: io.kestra.plugin.googleworkspace.sheets.CreateSpreadsheet
                    serviceAccount: "{{ inputs.serviceAccount }}"
			    """
		)
	}
)
public class CreateSpreadsheet extends AbstractSheet implements RunnableTask<CreateSpreadsheet.Output> {

	@Schema(
		title = "Spreadsheet title."
	)
	@NotNull
	private Property<String> title;

	@Override
	public Output run(RunContext runContext) throws Exception {
		Sheets service = this.connection(runContext);
        Logger logger = runContext.logger();

		SpreadsheetProperties properties = new SpreadsheetProperties()
			.setTitle(runContext.render(this.title).as(String.class).orElseThrow());

		Spreadsheet spreadsheet = new Spreadsheet()
			.setProperties(properties);

		Spreadsheet response = service
			.spreadsheets()
			.create(spreadsheet)
			.execute();

        logger.debug("Created spreadsheet '{}'", response.getSpreadsheetId());

		return Output.builder()
			.spreadsheetId(response.getSpreadsheetId())
			.spreadsheetUrl(response.getSpreadsheetUrl())
			.build();
	}

	@Getter
	@Builder
	public static class Output implements io.kestra.core.models.tasks.Output {

		@Schema(
			title = "The spreadsheet id."
		)
		private String spreadsheetId;

		@Schema(
			title = "The spreadsheet url."
		)
		private String spreadsheetUrl;


	}

}
