package io.kestra.plugin.googleworkspace.sheets;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.AppendValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Write file to a google Sheet"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "spreadsheetId: \"1Dkd3W0OQo-wxz1rrORLP7YGSj6EBLEg74fiTdbJUIQE\"",
                "range: \"Second One!A1:I\"",
                "writeOperation: APPEND",
                "insertData: OVERWRITE",
                "dataSeparator: \";\"",
                "valueInput: RAW"
            }
        )
    }
)
public class WriteFile extends AbstractWrite implements RunnableTask<AbstractWrite.Output> {

    @Schema(
        title = "The file to be write to the sheet."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "The data separator by which the data will be divided after the cells."
    )
    @PluginProperty()
    @Nullable
    private String dataSeparator;

    @Override
    public Output run(RunContext runContext) throws Exception {
        URI from = URI.create(runContext.render(this.getFrom()));
        InputStream inputStream = runContext.storage().getFile(from);

        String targetDataSeparator = runContext.render(this.getDataSeparator());
        List<List<Object>> fileData = readFileData(inputStream, targetDataSeparator);
        ValueRange valueRange = new ValueRange().setValues(fileData);

        runContext.metric(Counter.of("cells", this.valueRangeTotalSize(valueRange)));

        Sheets service = this.connection(runContext);

        return switch (this.writeOperation) {
            case UPDATE -> prepareUpdate(valueRange)
                .andThen(Sheets.Spreadsheets.Values.Update::execute)
                .andThen(this::createOutput)
                .apply(service);
            case APPEND -> prepareAppend(valueRange)
                .andThen(Sheets.Spreadsheets.Values.Append::execute)
                .andThen(AppendValuesResponse::getUpdates)
                .andThen(this::createOutput)
                .apply(service);
        };
    }

    private List<List<Object>> readFileData(final InputStream inputStream, final String targetDataSeparator) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        List<List<Object>> fileData = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            if (this.getDataSeparator() == null) {
                fileData.add(List.of(line));
            } else {
                List<Object> result = Arrays.stream(line.split(targetDataSeparator))
                    .map(value -> (Object) value)
                    .toList();
                fileData.add(result);
            }
        }
        reader.close();
        return fileData;
    }

}
