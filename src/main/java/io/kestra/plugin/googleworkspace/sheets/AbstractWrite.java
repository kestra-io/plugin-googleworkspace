package io.kestra.plugin.googleworkspace.sheets;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.UpdateValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.googleworkspace.sheets.type.DateTimeRender;
import io.kestra.plugin.googleworkspace.sheets.type.InsertData;
import io.kestra.plugin.googleworkspace.sheets.type.ValueInput;
import io.kestra.plugin.googleworkspace.sheets.type.ValueRender;
import io.kestra.plugin.googleworkspace.utils.CheckedOperation;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Write to the sheets"
)
public abstract class AbstractWrite extends AbstractSheet {

    @Schema(
        title = "The spreadsheet unique id."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    protected String spreadsheetId;

    @Schema(
        title = "The range of the values to write."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    protected String range;

    @Schema(
        title = "Type of write operation."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    protected WriteOperation writeOperation;

    @Schema(
        title = "Determines how the input data should be inserted.",
        description = "Required when the write operations is of type APPEND.\n" +
            "More details [here](https://developers.google.com/sheets/api/reference/rest/v4/ValueInputOption)"
    )
    @Nullable
    @PluginProperty(dynamic = true)
    protected InsertData insertData;

    @Schema(
        title = "Determines how input data should be interpreted.",
        description = "More details [here](https://developers.google.com/sheets/api/reference/rest/v4/ValueInputOption)"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    protected ValueInput valueInput;

    @Schema(
        title = "Determines if the update response should include the values of the cells that were updated.\n" +
            "By default, responses do not include the updated values."
    )
    @PluginProperty()
    @Builder.Default
    protected final boolean includeValuesInResponse = false;

    @Schema(
        title = "Determines how values should be rendered.",
        description = "More details [here](https://developers.google.com/sheets/api/reference/rest/v4/ValueRenderOption)"
    )
    @NotNull
    @Builder.Default
    @PluginProperty()
    protected ValueRender valueRender = ValueRender.UNFORMATTED_VALUE;

    @Schema(
        title = "How dates, times, and durations should be rendered.",
        description = "This is ignored if valueRender is `FORMATTED_VALUE`.\n" +
            "More details [here](https://developers.google.com/sheets/api/reference/rest/v4/DateTimeRenderOption)"
    )
    @NotNull
    @Builder.Default
    @PluginProperty()
    protected DateTimeRender dateTimeRender = DateTimeRender.FORMATTED_STRING;

    protected Integer valueRangeTotalSize(final ValueRange valueRange) {
        return valueRange.getValues().stream().mapToInt(List::size).sum();
    }

    protected CheckedOperation<Sheets, Sheets.Spreadsheets.Values.Update, IOException> prepareUpdate(final ValueRange valueRange) {
        return sheets -> sheets.spreadsheets().values().update(
                this.spreadsheetId,
                this.range,
                valueRange
            ).setIncludeValuesInResponse(this.includeValuesInResponse)
            .setResponseDateTimeRenderOption(this.dateTimeRender.name())
            .setResponseValueRenderOption(this.valueRender.name())
            .setValueInputOption(this.valueInput.name());
    }

    protected CheckedOperation<Sheets, Sheets.Spreadsheets.Values.Append, IOException> prepareAppend(final ValueRange valueRange) {
        return sheets -> sheets.spreadsheets().values().append(
                this.spreadsheetId,
                this.range,
                valueRange
            ).setIncludeValuesInResponse(this.includeValuesInResponse)
            .setResponseDateTimeRenderOption(this.dateTimeRender.name())
            .setResponseValueRenderOption(this.valueRender.name())
            .setValueInputOption(this.valueInput.name())
            .setInsertDataOption(Objects.requireNonNull(this.insertData).name());
    }

    protected Output createOutput(final UpdateValuesResponse response) {
        return Output.builder()
            .updatedRows(response.getUpdatedRows())
            .updatedColumns(response.getUpdatedColumns())
            .updatedRange(response.getUpdatedRange())
            .updatedCells(response.getUpdatedCells())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The number of rows where at least one cell in the row was updated."
        )
        private int updatedRows;

        @Schema(
            title = "The number of columns where at least one cell in the column was updated."
        )
        private int updatedColumns;

        @Schema(
            title = "The number of cells updated."
        )
        private int updatedCells;

        @Schema(
            title = "The range that updates were applied to."
        )
        private String updatedRange;
    }

    public enum WriteOperation {
        UPDATE,
        APPEND
    }
}
