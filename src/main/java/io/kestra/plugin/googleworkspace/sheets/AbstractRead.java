package io.kestra.plugin.googleworkspace.sheets;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read from a sheets"
)
public abstract class AbstractRead extends AbstractSheet {
    @Schema(
        title = "The spreadsheet unique id"
    )
    @NotNull
    protected Property<String> spreadsheetId;

    @Schema(
        title = "Determines how values should be rendered in the output.",
        description = "More details [here](https://developers.google.com/sheets/api/reference/rest/v4/ValueRenderOption)"
    )
    @NotNull
    @Builder.Default
    protected Property<ValueRender> valueRender = Property.of(ValueRender.UNFORMATTED_VALUE);

    @Schema(
        title = "How dates, times, and durations should be represented in the output.",
        description = "his is ignored if valueRender is `FORMATTED_VALUE`.\n" +
            "More details [here](https://developers.google.com/sheets/api/reference/rest/v4/DateTimeRenderOption)"
    )
    @NotNull
    @Builder.Default
    protected Property<DateTimeRender> dateTimeRender = Property.of(DateTimeRender.FORMATTED_STRING);

    @Builder.Default
    @Schema(
        title = "Specifies if the first line should be the header (default: false)"
    )
    protected final Property<Boolean> header = Property.of(true);

    @Schema(
        title = "Whether to Fetch the data from the query result to the task output"
    )
    @Builder.Default
    protected final Property<Boolean> fetch = Property.of(false);

    @Schema(
        title = "Whether to store the data from the query result into an ion serialized data file"
    )
    @Builder.Default
    protected final Property<Boolean> store = Property.of(true);

    protected List<Object> transform(List<List<Object>> values, RunContext runContext) throws IllegalVariableEvaluationException {
        List<Object> result = new ArrayList<>();

        if (values == null || values.isEmpty()) {
            return result;
        }

        if (runContext.render(this.header).as(Boolean.class).orElseThrow()) {
            List<Object> headers = values.get(0);

            for (int i = 1; i < values.size(); i++) {
                List<Object> row = values.get(i);
                    Map<String, Object> resultRows = new LinkedHashMap<>();

                    for (int j = 0; j < headers.size(); j++) {
                        resultRows.put((String) headers.get(j), j < row.size() ? row.get(j) : null);
                    }
                    result.add(resultRows);
            }
        } else {
            result.addAll(values);
        }

        return result;
    }

    protected File store(RunContext runContext, List<Object> values) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (
            var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            var flux = Flux.fromIterable(values);
            FileSerde.writeAll(output, flux).block();
        }
        return tempFile;
    }

    public enum ValueRender {
        FORMATTED_VALUE,
        UNFORMATTED_VALUE,
        FORMULA
    }

    public enum DateTimeRender {
        SERIAL_NUMBER,
        FORMATTED_STRING
    }
}
