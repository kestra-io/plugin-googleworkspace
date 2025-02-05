package io.kestra.plugin.googleworkspace.sheets;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.DuplicateHeaderMode;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class DataParser {

	private final RunContext runContext;

	public List<List<Object>> parseCsv(InputStream inputStream, AbstractLoad.CsvOptions csvOptions) throws IOException, IllegalVariableEvaluationException {
		List<List<Object>> result = new ArrayList<>();

		Charset charset = Charset.forName(
			runContext.render(csvOptions.getEncoding()).as(String.class).orElse(null)
		);

		InputStreamReader reader = new InputStreamReader(inputStream, charset);
		CSVFormat format = getCsvFormat(csvOptions);
		try (CSVParser parser = CSVParser.builder().setFormat(format).setReader(reader).get()) {
			for (CSVRecord csvRecord : parser) {
				List<Object> row = new ArrayList<>();

				csvRecord.forEach(row::add);
				result.add(row);
			}
			return result;
		}
	}

	public List<List<Object>> parseThroughMapper(InputStream inputStream, ObjectMapper mapper, boolean includeHeaders) throws IOException {
		List<List<Object>> result = new ArrayList<>();

		List<Map<String, Object>> list = mapper.readValue(inputStream, List.class);

		if (includeHeaders) {
			List<ArrayList<Object>> headers = list.stream()
				.map(Map::keySet)
				.map(column -> new ArrayList<Object>(column))
				.distinct()
				.toList();

			result.addAll(headers);
		}

		result.addAll(
			list.stream()
				.map(Map::values)
				.map(ArrayList::new)
				.toList()
		);

		return result;
	}

	public List<List<Object>> parseAvro(InputStream inputStream, boolean includeHeaders, String avroSchema) throws IOException {
		List<List<Object>> result = new ArrayList<>();

		boolean isHeaderIncluded = false;
		try (DataFileStream<GenericRecord> reader = new DataFileStream<>(inputStream, new GenericDatumReader<>())) {
			Schema schema;
			if (avroSchema != null) {
				schema = new Schema.Parser().parse(avroSchema);
			} else {
				schema = reader.getSchema();
			}

			while (reader.hasNext()) {
				GenericRecord genericRecord = reader.next();
				List<Object> row = new ArrayList<>();

				if (includeHeaders && !isHeaderIncluded) {
					List<Object> headers = new ArrayList<>(
						schema.getFields()
							.stream()
							.map(Schema.Field::name)
							.toList()
					);

					isHeaderIncluded = true;
					result.add(headers);
				}

				schema.getFields()
					.forEach(field -> row.add(genericRecord.get(field.name()).toString()));

				result.add(row);
			}
		}

		return result;
	}

	public List<List<Object>> parseParquet(InputStream inputStream, boolean includeHeaders) throws IOException {
		List<List<Object>> result = new ArrayList<>();

		File tempFile = File.createTempFile("temp-parquet", ".parquet");
		tempFile.deleteOnExit();

		try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
			IOUtils.copy(inputStream, outputStream);
		}

		boolean isHeaderIncluded = false;
		Configuration configuration = new Configuration();
		LocalInputFile inputFile = new LocalInputFile(tempFile.toPath());
		try (ParquetReader<GenericRecord> reader = AvroParquetReader
			.<GenericRecord>builder(inputFile).withConf(configuration).build()
		) {
			GenericRecord genericRecord;
			while ((genericRecord = reader.read())!= null) {
				List<Object> row = new ArrayList<>();

				if (includeHeaders && !isHeaderIncluded) {
					List<Object> headers = new ArrayList<>(
						genericRecord.getSchema()
							.getFields()
							.stream()
							.map(Schema.Field::name)
							.toList()
					);

					isHeaderIncluded = true;
					result.add(headers);
				}

				GenericRecord finalRecord = genericRecord;
				genericRecord.getSchema()
					.getFields()
					.forEach(field -> row.add(
						finalRecord.get(field.name()).toString())
					);

				result.add(row);
			}
		}

		return result;
	}

	public List<List<Object>> parseORC(InputStream inputStream, boolean includeHeaders) throws IOException {
		List<List<Object>> result = new ArrayList<>();

		File tempFile = File.createTempFile("temp-orc", ".orc");
		tempFile.deleteOnExit();

		try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
			IOUtils.copy(inputStream, outputStream);
		}

		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);
		Path path = new Path(tempFile.getPath());
		try (Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(configuration).filesystem(fileSystem))) {
			TypeDescription schema = reader.getSchema();
			VectorizedRowBatch rowBatch = schema.createRowBatch();

			try (RecordReader rows = reader.rows()) {
				if (includeHeaders) {
					result.add(
						new ArrayList<>(
							schema.getFieldNames()
						)
					);
				}

				while (rows.nextBatch(rowBatch)) {
					for (int row = 0; row < rowBatch.size; row++) {
						List<Object> records = new ArrayList<>();

						for (ColumnVector vector : rowBatch.cols) {
							records.add(
								getValue(vector, row)
							);
						}

						result.add(records);
					}
				}
			}
		}

		return result;
	}

	private Object getValue(ColumnVector vector, int row) {
		return switch (vector) {
			case LongColumnVector longVector -> {
				if (vector instanceof DateColumnVector dateVector) {
					yield dateVector.formatDate(row);
				} else {
					yield longVector.vector[row];
				}
			}
			case DoubleColumnVector doubleVector -> doubleVector.vector[row];
			case BytesColumnVector bytesVector -> {
				if (!bytesVector.isNull[row]) {
					int start = bytesVector.start[row];
					int length = bytesVector.length[row];
					yield new String(bytesVector.vector[row], start, length);
				} else {
					yield "";
				}
			}
			case DecimalColumnVector decimalVector -> decimalVector.vector[row];
			case TimestampColumnVector timeVector -> timeVector.time[row];
			case null, default -> "Unsupported type";
		};
	}

	private CSVFormat getCsvFormat(AbstractLoad.CsvOptions csvOptions) throws IllegalVariableEvaluationException {
		return CSVFormat.Builder.create()
            .setDelimiter(
                csvOptions.getFieldDelimiter() != null ?
                    this.runContext.render(csvOptions.getFieldDelimiter()).as(String.class).orElseThrow() :
                    CSVFormat.DEFAULT.getDelimiterString()
            )
            .setQuote(
                csvOptions.getQuote() != null ?
                    this.runContext.render(csvOptions.getQuote()).as(String.class).orElseThrow().charAt(0) :
                    CSVFormat.DEFAULT.getQuoteCharacter()
            )
            .setRecordSeparator(CSVFormat.DEFAULT.getRecordSeparator())
            .setIgnoreEmptyLines(true)
            .setDuplicateHeaderMode(DuplicateHeaderMode.ALLOW_EMPTY)
            .setSkipHeaderRecord(csvOptions.getSkipLeadingRows() != null && runContext.render(csvOptions.getSkipLeadingRows()).as(Long.class).orElseThrow() > 0).get();
	}

}
