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
    title = "Read data from a Google Sheet",
    description = "Base class for reading values with configurable render options, header handling, and optional fetch/store output"
)
public abstract class AbstractRead extends AbstractSheet {
    @Schema(
        title = "Spreadsheet ID"
    )
    @NotNull
    protected Property<String> spreadsheetId;

    @Schema(
        title = "Value render option",
        description = "FORMATTED_VALUE, UNFORMATTED_VALUE (default), or FORMULA"
    )
    @NotNull
    @Builder.Default
    protected Property<ValueRender> valueRender = Property.ofValue(ValueRender.UNFORMATTED_VALUE);

    @Schema(
        title = "Date/time render option",
        description = "SERIAL_NUMBER or FORMATTED_STRING (default); ignored if valueRender=FORMATTED_VALUE"
    )
    @NotNull
    @Builder.Default
    protected Property<DateTimeRender> dateTimeRender = Property.ofValue(DateTimeRender.FORMATTED_STRING);

    @Builder.Default
    @Schema(
        title = "Treat first row as header",
        description = "When true, maps rows to objects using first row keys; default true"
    )
    protected final Property<Boolean> header = Property.ofValue(true);

    @Schema(
        title = "Fetch results",
        description = "If true, data is returned in output; otherwise written to storage"
    )
    @Builder.Default
    protected final Property<Boolean> fetch = Property.ofValue(false);

    @Schema(
        title = "Store results",
        description = "If true, writes ION file to storage; default true"
    )
    @Builder.Default
    protected final Property<Boolean> store = Property.ofValue(true);

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
