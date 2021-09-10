package io.kestra.plugin.googleworkspace.sheets;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.*;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read all sheets from a google Sheets"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "spreadsheetId: \"1Dkd3W0OQo-wxz1rrORLP7YGSj6EBLEg74fiTdbJUIQE\"",
                "store: true",
                "valueRender: FORMATTED_VALUE"
            }
        )
    }
)
public class Read extends AbstractRead implements RunnableTask<Read.Output> {
    @Override
    public Read.Output run(RunContext runContext) throws Exception {
        Sheets service = this.connection(runContext);
        Logger logger = runContext.logger();

        Spreadsheet spreadsheet = service.spreadsheets()
            .get(runContext.render(spreadsheetId))
            .execute();

        runContext.metric(Counter.of("sheets", spreadsheet.getSheets().size()));

        // generate range
        List<String> ranges = spreadsheet
            .getSheets()
            .stream()
            .map(s -> s.getProperties().getTitle() + "!R1C1:" +
                "R" + s.getProperties().getGridProperties().getRowCount() +
                "C" + s.getProperties().getGridProperties().getColumnCount())
            .collect(Collectors.toList());

        // batch get all ranges
        BatchGetValuesResponse batchGet = service.spreadsheets().values()
            .batchGet(spreadsheetId)
            .setRanges(ranges)
            .set("valueRenderOption", this.valueRender.name())
            .set("dateTimeRenderOption", this.dateTimeRender.name())
            .execute();

        // read values
        Map<String, List<Object>> rows = new HashMap<>();
        Map<String, URI> uris = new HashMap<>();
        AtomicInteger rowsCount = new AtomicInteger();

        for(int index = 0; index < spreadsheet.getSheets().size(); index++) {
            ValueRange valueRange = batchGet.getValueRanges().get(index);
            Sheet sheet = spreadsheet.getSheets().get(index);

            logger.info("Fetch {} rows from range '{}'", valueRange.getValues().size(), valueRange.getRange());
            rowsCount.addAndGet(valueRange.getValues().size());

            List<Object> values = this.transform(valueRange.getValues());

            if (this.fetch) {
                rows.put(sheet.getProperties().getTitle(), values);
            } else {
                uris.put(sheet.getProperties().getTitle(), runContext.putTempFile(this.store(runContext, values)));
            }
        }

        Output.OutputBuilder builder = Output.builder()
            .size(rowsCount.get());

        if (this.fetch) {
            builder.rows(rows);
        } else {
            builder.uris(uris);
        }

        runContext.metric(Counter.of("rows", rowsCount.get()));

        return builder
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List containing the fetched data",
            description = "Only populated if 'fetch' parameter is set to true.\n" +
                "The key is the name of the sheet"
        )
        @PluginProperty(additionalProperties = List.class)
        private Map<String, List<Object>> rows;

        @Schema(
            title = "The size of the rows fetch"
        )
        private int size;

        @Schema(
            title = "The uri of store result",
            description = "The keyOnly populated if 'store' is set to true.\n" +
                "The key is the name of the sheet"
        )
        @PluginProperty(additionalProperties = URI.class)
        private Map<String, URI> uris;
    }

}
