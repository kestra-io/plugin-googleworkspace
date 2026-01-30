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
        title = "Spreadsheet ID",
        description = "Target spreadsheet to load data into"
    )
    @NotNull
    protected Property<String> spreadsheetId;

    @Builder.Default
    @Schema(
        title = "Treat first row as header",
        description = "When true, first row becomes column names; default false"
    )
    protected final Property<Boolean> header = Property.ofValue(false);

    @Schema(
        title = "CSV parsing options"
    )
    @Builder.Default
    private CsvOptions csvOptions = CsvOptions.builder().build();

    @Schema(
        title = "Avro schema",
        description = "Optional schema string to read Avro payloads"
    )
    private Property<String> avroSchema;

    @Schema(
        title = "Input file format",
        description = "Optional override; otherwise inferred from file extension"
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
            title = "CSV field delimiter",
            description = "Single-character separator; default comma"
        )
        @Builder.Default
        private Property<String> fieldDelimiter = Property.ofValue(",");

        @Schema(
            title = "Skip leading rows",
            description = "Number of initial rows to skip; default 0"
        )
        @PluginProperty
        private Property<Long> skipLeadingRows;

        @Schema(
            title = "CSV quote character"
        )
        private Property<String> quote;

        @Schema(
            title = "CSV file encoding",
            description = "Character set used; default UTF-8"
        )
        @Builder.Default
        private Property<String> encoding = Property.ofValue("UTF-8");

    }
}
