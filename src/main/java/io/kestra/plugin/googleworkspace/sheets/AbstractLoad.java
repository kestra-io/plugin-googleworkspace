package io.kestra.plugin.googleworkspace.sheets;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read from a sheets"
)
public abstract class AbstractLoad extends AbstractSheet {

    private final static ObjectMapper JSON_MAPPER = JacksonMapper.ofJson();
    private final static ObjectMapper ION_MAPPER = JacksonMapper.ofIon();

    @Schema(
        title = "The spreadsheet unique id"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    protected String spreadsheetId;

    @Builder.Default
    @Schema(
        title = "Specifies if the first line should be the header (default: false)"
    )
    protected final Boolean header = false;

    @Schema(
        title = "Csv parsing options (Optional)."
    )
    @Builder.Default
    private CsvOptions csvOptions = CsvOptions.builder().build();

    @Schema(
        title = "Schema to read avro objects (Optional).",
        description = "If provided, task will read avro objects from this schema."
    )
    @PluginProperty(dynamic = true)
    private String avroSchema;

    @Schema(
        title = "Format of input file.",
        description = "If not provided task will programmatically try to find correct format, base on extension"
    )
    @PluginProperty(dynamic = true)
    private Format format;

    protected List<List<Object>> parse(RunContext runContext, String from) throws Exception {
        Format format;
        if (this.format == null) {
            format = Format.getFromFile(new File(from));
        } else {
            format = this.format;
        }

	    URI fromURI = new URI(runContext.render(from));

        File file = runContext.workingDir().createTempFile(FilenameUtils.getExtension(from)).toFile();
        try (FileOutputStream out = new FileOutputStream(file)) {
            IOUtils.copy(runContext.storage().getFile(fromURI), out);
        }

        DataParser parser = new DataParser(runContext);
        return switch (format) {
            case ION -> parser.parseThroughMapper(file, ION_MAPPER, header);
            case JSON -> parser.parseThroughMapper(file, JSON_MAPPER, header);
            case CSV -> parser.parseCsv(file, csvOptions);
            case AVRO -> parser.parseAvro(file, header, runContext.render(this.avroSchema));
            case PARQUET -> parser.parseParquet(file, header);
            case ORC -> parser.parseORC(file, header);
        };
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

        public static Format getFromFile(File file) {
            String fileName = file.getName();
            String extension = FilenameUtils.getExtension(fileName);
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
        @PluginProperty(dynamic = true)
        private String fieldDelimiter = ",";

        @Schema(
            title = "The number of rows at the top of a CSV file that will be skipped when reading the data.",
            description = "The default value is 0. This property is useful if you have header rows in the file" +
                " that should be skipped."
        )
        @PluginProperty
        private Long skipLeadingRows;

        @Schema(
            title = "The quote character in a CSV file."
        )
        @PluginProperty(dynamic = true)
        private String quote;

        @Schema(
            title = "The file encoding of CSV file."
        )
        @Builder.Default
        @PluginProperty(dynamic = true)
        private String encoding = "UTF-8";

    }
}
