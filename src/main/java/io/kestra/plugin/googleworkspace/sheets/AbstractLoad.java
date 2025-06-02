package io.kestra.plugin.googleworkspace.sheets;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;

import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractLoad extends AbstractSheet {

    private final static ObjectMapper JSON_MAPPER = JacksonMapper.ofJson();
    private final static ObjectMapper ION_MAPPER = JacksonMapper.ofIon();

    @Schema(
        title = "The spreadsheet unique id."
    )
    @NotNull
    protected Property<String> spreadsheetId;

    @Builder.Default
    @Schema(
        title = "Specifies if the first line should be the header (default: false)."
    )
    protected final Property<Boolean> header = Property.ofValue(false);

    @Schema(
        title = "Csv parsing options (Optional)."
    )
    @Builder.Default
    private CsvOptions csvOptions = CsvOptions.builder().build();

    @Schema(
        title = "Schema for avro objects (Optional).",
        description = "If provided, the task will read avro objects using this schema."
    )
    private Property<String> avroSchema;

    @Schema(
        title = "Format of the input file.",
        description = "If not provided, the task will programmatically try to find the correct format based on the extension."
    )
    private Property<Format> format;

    protected List<List<Object>> parse(RunContext runContext, URI from) throws Exception {
        Format format;
        if (this.format == null) {
            format = Format.getFromFile(from.toString());
        } else {
            format = runContext.render(this.format).as(Format.class).orElseThrow();
        }

        try (InputStream inputStream = runContext.storage().getFile(from)) {
            DataParser parser = new DataParser(runContext);
            var headerValue = runContext.render(header).as(Boolean.class).orElseThrow();
            return switch (format) {
                case ION -> parser.parseThroughMapper(inputStream, ION_MAPPER, headerValue);
                case JSON -> parser.parseThroughMapper(inputStream, JSON_MAPPER, headerValue);
                case CSV -> parser.parseCsv(inputStream, csvOptions);
                case AVRO -> parser.parseAvro(inputStream, headerValue, runContext.render(this.avroSchema).as(String.class).orElse(null));
                case PARQUET -> parser.parseParquet(inputStream, headerValue);
                case ORC -> parser.parseORC(inputStream, headerValue);
            };
        }
    }

    @Getter
    @RequiredArgsConstructor
    private enum Format {
        ION("ion"),
        CSV("csv"),
        AVRO("avro"),
        PARQUET("parquet"),
        ORC("orc"),
        JSON("json");

        private final String extension;

        public static Format getFromFile(String uri) {
            String extension = FilenameUtils.getExtension(uri);
            return Format.getFromExtension(extension);
        }

        private static Format getFromExtension(String extension) {
            return Arrays.stream(values())
                .filter(format -> format.extension.equalsIgnoreCase(extension))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Not supported format"));
        }
    }

    @Builder
    @ToString
    @EqualsAndHashCode
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CsvOptions {

        @Schema(
            title = "The separator for fields in a CSV file."
        )
        @Builder.Default
        private Property<String> fieldDelimiter = Property.ofValue(",");

        @Schema(
            title = "The number of rows at the top of a CSV file that will be skipped when reading the data.",
            description = "The default value is 0. This property is useful if you have header rows in the file" +
                " that should be skipped."
        )
        @PluginProperty
        private Property<Long> skipLeadingRows;

        @Schema(
            title = "The quote character in a CSV file."
        )
        private Property<String> quote;

        @Schema(
            title = "The file encoding of CSV file."
        )
        @Builder.Default
        private Property<String> encoding = Property.ofValue("UTF-8");

    }
}
