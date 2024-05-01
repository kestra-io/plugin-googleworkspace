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
import io.kestra.core.utils.Rethrow;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Write value to a google Sheet"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "spreadsheetId: \"1Dkd3W0OQo-wxz1rrORLP7YGSj6EBLEg74fiTdbJUIQE\"",
                "range: \"Second One!A1:I\"",
                "writeOperation: UPDATE",
                "valueInput: RAW"
            }
        )
    }
)
public class WriteValue extends AbstractWrite implements RunnableTask<AbstractWrite.Output> {

    @Schema(
        title = "One or more value(s) to be write to the sheet.",
        description = "It can be a string or an array of strings.",
        anyOf = {
            String.class,
            String[].class
        }
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object value;

    @Schema(
        title = "Represents the direction of operations on an array."
    )
    @NotNull
    @Builder.Default
    @PluginProperty()
    protected WriteValue.ArrayDirection arrayDirection = ArrayDirection.ROWS;

    @SneakyThrows
    @Override
    public Output run(RunContext runContext) throws Exception {
        ValueRange valueRange = convertValue(runContext::render);

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

    private ValueRange convertValue(final Rethrow.FunctionChecked<String, String, Exception> renderFunction) throws Exception {
        if (this.value instanceof String) {
            String targetValue = renderFunction.apply((String) this.value);
            List<Object> values = List.of(targetValue);
            return new ValueRange().setValues(List.of(values));
        } else if (this.value instanceof Collection) {
            Collection<String> values = (Collection<String>) this.value;
            List<Object> targetValues = new ArrayList<>();
            for (Object value : values) {
                targetValues.add(renderFunction.apply((String) value));
            }
            List<List<Object>> valueslist = switch (arrayDirection) {
                case ROWS -> List.of(targetValues);
                case COLUMNS -> transform(targetValues);
            };
            return new ValueRange().setValues(valueslist);
        } else {
            throw new IllegalArgumentException("Invalid value type '" + this.value.getClass() + "'");
        }
    }

    private List<List<Object>> transform(final List<Object> values) {
        List<List<Object>> targetValues = new ArrayList<>();
        for (Object value : values) {
            targetValues.add(List.of(value));
        }
        return targetValues;
    }

    public enum ArrayDirection {
        ROWS,
        COLUMNS
    }

}
